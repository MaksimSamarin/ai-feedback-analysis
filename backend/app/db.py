from __future__ import annotations

import ctypes
import gc
import json
import hashlib
import logging
import math
import os
import re
import time
import uuid
from contextlib import contextmanager, nullcontext
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any, Iterator

from app.config import (
    CACHE_MAINTENANCE_INTERVAL_SEC,
    HASH_PARTITIONS,
    GROUP_MAX_ROWS,
    MAX_LLM_CACHE_ROWS,
    MAX_SEMANTIC_CACHE_ROWS,
    RESULTS_DIR,
    UPLOAD_ORPHAN_TTL_HOURS,
    PAUSED_REPORT_TTL_DAYS,
)
try:
    from redis import ConnectionPool as RedisConnectionPool, Redis
    from redis.backoff import ExponentialBackoff
    from redis.exceptions import BusyLoadingError
    from redis.exceptions import ConnectionError as RedisConnectionError
    from redis.exceptions import TimeoutError as RedisTimeoutError
    from redis.retry import Retry
except Exception:  # pragma: no cover
    Redis = None  # type: ignore[assignment]
    RedisConnectionPool = None  # type: ignore[assignment]
    ExponentialBackoff = None  # type: ignore[assignment]
    Retry = None  # type: ignore[assignment]
    BusyLoadingError = None  # type: ignore[assignment]
    RedisConnectionError = None  # type: ignore[assignment]
    RedisTimeoutError = None  # type: ignore[assignment]
try:
    from psycopg.rows import dict_row
    from psycopg_pool import ConnectionPool as PgConnectionPool
except Exception:  # pragma: no cover
    dict_row = None  # type: ignore[assignment]
    PgConnectionPool = None  # type: ignore[assignment]

logger = logging.getLogger("review_analyzer.db")
_last_cache_maintenance_ts = 0.0
_REDIS_URL = os.getenv("REDIS_URL", "").strip()
DATABASE_URL = os.getenv("DATABASE_URL", "").strip()
DB_POOL_MIN_SIZE = max(1, int(os.getenv("DB_POOL_MIN_SIZE", "1")))
DB_POOL_MAX_SIZE = max(DB_POOL_MIN_SIZE, int(os.getenv("DB_POOL_MAX_SIZE", "12")))
DB_STATEMENT_TIMEOUT_MS = max(0, int(os.getenv("DB_STATEMENT_TIMEOUT_MS", "0" if os.getenv("WORKER_MODE") else "60000")))
_PG_POOL: PgConnectionPool | None = None
_REDIS_POOL: RedisConnectionPool | None = None
_REDIS_CLIENT: Redis | None = None
SOFT_DELETE_MIGRATION = "reports_soft_delete_v2_2_0"


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_reliable_since(conn: Any) -> str | None:
    row = conn.execute(
        "SELECT applied_at FROM schema_migrations WHERE name=?",
        (SOFT_DELETE_MIGRATION,),
    ).fetchone()
    return row["applied_at"] if row else None


def get_redis_client() -> Redis | None:
    """Возвращает общий Redis-клиент с пулом соединений.

    Переиспользует один клиент на процесс — чтобы не создавать TCP-соединение
    при каждом вызове (см. BUG-03). Возвращает None если Redis недоступен.

    Встроенный retry (v2.1.2, BUG-20): прозрачно ретраит транзиентные ошибки
    Redis на уровне библиотеки — ConnectionError (порт закрыт после OOM-kill),
    BusyLoadingError (RDB grузится после рестарта), TimeoutError (зависший
    запрос). До этого фикса worker при таких ошибках падал и помечал job в
    `failed` — пользователи теряли отчёты на середине обработки (см. BUG-19
    про OOM Redis от llm_cache:*).

    Параметры:
    - 5 попыток retry с экспоненциальным backoff (base=1, cap=10 секунд)
    - суммарное окно ожидания ~25 секунд — покрывает типичный RDB-load 10–30с
    - socket_timeout=10s — защита от зависших запросов (Redis иногда тупит
      при загрузке большого RDB), без таймаута запрос блокировал бы воркер
      навсегда
    - socket_connect_timeout=5s — быстрая реакция на «порт закрыт»
    """
    global _REDIS_POOL, _REDIS_CLIENT
    if Redis is None or RedisConnectionPool is None:
        return None
    if not _REDIS_URL:
        return None
    try:
        if _REDIS_CLIENT is None:
            if _REDIS_POOL is None:
                # retry-объект может быть None, если redis-py не импортировался
                # (Retry присвоен None в верхнем except) — тогда падаем обратно
                # на стандартный пул без retry.
                retry_obj = None
                if Retry is not None and ExponentialBackoff is not None:
                    retry_obj = Retry(ExponentialBackoff(cap=10, base=1), 5)
                retry_on_error: list = []
                for exc_cls in (BusyLoadingError, RedisConnectionError, RedisTimeoutError):
                    if exc_cls is not None:
                        retry_on_error.append(exc_cls)
                pool_kwargs: dict[str, Any] = {
                    "decode_responses": True,
                    "socket_timeout": 10,
                    "socket_connect_timeout": 5,
                }
                if retry_obj is not None:
                    pool_kwargs["retry"] = retry_obj
                if retry_on_error:
                    pool_kwargs["retry_on_error"] = retry_on_error
                _REDIS_POOL = RedisConnectionPool.from_url(_REDIS_URL, **pool_kwargs)
            _REDIS_CLIENT = Redis(connection_pool=_REDIS_POOL)
        return _REDIS_CLIENT
    except Exception:
        return None


# ───────────────────── Redis-list для логов задач ─────────────────────────
# Логи во время обработки worker'ом пишет в Redis-list job_logs:<job_id>
# (LPUSH + LTRIM до 50 строк, TTL 24 часа). Через GET /api/reports/active
# backend читает последние 20 записей и отдаёт их фронту — UI блок «3.
# Обработка» рендерит этот хвост. После v2.1.0 — основной канал доставки
# логов вместо SSE-подписки на job_events:<id>.

JOB_LOGS_MAX = 50
JOB_LOGS_TTL_SEC = 86400  # 24 часа после последней записи


def _job_logs_key(job_id: str) -> str:
    return f"job_logs:{job_id}"


_LIBC: ctypes.CDLL | None
try:
    _LIBC = ctypes.CDLL("libc.so.6")
except Exception:
    _LIBC = None


def release_unused_memory() -> int:
    """Освобождает накопленный «мусор» Python и возвращает свободные арены glibc в ОС.

    Вызывается воркером после каждой завершённой задачи (см. worker.py main-loop)
    чтобы держать RSS в разумных рамках. Без этой чистки long-running Python
    на glibc накапливает фрагментацию арен malloc — за неделю работы воркер
    раздувается с базовых ~100 МБ до 900+ МБ, упирается в `mem_limit` и
    убивается OOM-killer'ом.

    Состоит из двух шагов:
      1. ``gc.collect()`` — собирает редкие циклические ссылки. На нашем коде
         почти ничего не делает (refcount освобождает 99% объектов сам),
         но дёшево.
      2. ``malloc_trim(0)`` — просит glibc отдать ОС свободные страницы из
         своих арен. Это и есть главный эффект — возвращает 30-50% RSS
         после длительной работы.

    Безопасна для не-glibc платформ (Alpine/musl): если ``libc.so.6`` не
    загрузился — возвращает -1, gc.collect всё равно выполняется.

    Возвращает результат ``malloc_trim`` (1 = strim выполнен, 0 = нечего
    отдать), -1 если функция недоступна или упала.
    """
    gc.collect()
    if _LIBC is None:
        return -1
    try:
        return int(_LIBC.malloc_trim(0))
    except Exception:
        return -1


def cleanup_legacy_llm_cache_redis_keys(batch_size: int = 1000) -> int:
    """Одноразовая миграция v2.1.2: удаляет накопленные `llm_cache:*` ключи в Redis.

    В v2.1.1 и ранее `put_cached_analysis` писал кэш LLM-ответов одновременно в
    Postgres и Redis (как L1). Redis-копия писалась без TTL и без enforce-лимита,
    за неделю накапливала ~1M ключей (~1 ГБ) и валила Redis OOM-kill'ом.

    В v2.1.2 L1-кэш полностью удалён из кода. Эта функция чистит legacy-ключи
    при старте worker'а — иначе после деплоя миллион ключей продолжит занимать
    память до ручного вмешательства (CI rolling-restart не сносит redis_data volume).

    После первого успешного прогона при следующих стартах SCAN возвращает пустой
    итератор → no-op. Безопасно вызывать многократно.

    Использует UNLINK (неблокирующее удаление, добавлено в Redis 4.0).
    Возвращает количество удалённых ключей.
    """
    client = get_redis_client()
    if client is None:
        return 0
    total_deleted = 0
    try:
        batch: list[str] = []
        for key in client.scan_iter(match="llm_cache:*", count=batch_size):
            batch.append(key)
            if len(batch) >= batch_size:
                client.unlink(*batch)
                total_deleted += len(batch)
                batch.clear()
        if batch:
            client.unlink(*batch)
            total_deleted += len(batch)
    except Exception:
        # Best-effort: даже если чистка не прошла полностью, остальной запуск
        # worker'а продолжается. При следующем старте попробуем снова.
        return total_deleted
    return total_deleted


def push_job_log(job_id: str, message: str) -> None:
    """Добавляет строку в Redis-list job_logs:<id>. Best-effort, не падает.

    Использовать рядом с in-memory job.add_log() — Redis-list нужен чтобы
    backend (отдельный процесс) видел логи worker'а в /api/reports/active.
    """
    if not job_id or not message:
        return
    client = get_redis_client()
    if client is None:
        return
    key = _job_logs_key(job_id)
    try:
        pipe = client.pipeline(transaction=False)
        pipe.lpush(key, message)
        pipe.ltrim(key, 0, JOB_LOGS_MAX - 1)
        pipe.expire(key, JOB_LOGS_TTL_SEC)
        pipe.execute()
    except Exception:
        # Логи — best-effort, ошибка Redis не должна валить worker.
        pass


def get_job_logs(job_id: str, limit: int = 20) -> list[str]:
    """Возвращает последние `limit` строк лога в хронологическом порядке."""
    if not job_id:
        return []
    client = get_redis_client()
    if client is None:
        return []
    try:
        # LPUSH добавляет в начало, LRANGE 0 N-1 возвращает N последних
        # сообщений (newest first). Переворачиваем — фронт ожидает от
        # старых к новым.
        items = client.lrange(_job_logs_key(job_id), 0, max(0, limit - 1))
        return list(reversed(items))
    except Exception:
        return []


def get_job_logs_bulk(job_ids: list[str], limit: int = 20) -> dict[str, list[str]]:
    """Pipeline-чтение логов для нескольких job_id за один round-trip."""
    result: dict[str, list[str]] = {jid: [] for jid in job_ids}
    if not job_ids:
        return result
    client = get_redis_client()
    if client is None:
        return result
    try:
        pipe = client.pipeline(transaction=False)
        for jid in job_ids:
            pipe.lrange(_job_logs_key(jid), 0, max(0, limit - 1))
        responses = pipe.execute()
        for jid, items in zip(job_ids, responses):
            result[jid] = list(reversed(items)) if isinstance(items, list) else []
    except Exception:
        pass
    return result


def _get_pg_pool() -> PgConnectionPool:
    global _PG_POOL
    if PgConnectionPool is None:
        raise RuntimeError("psycopg_pool is not installed")
    if not DATABASE_URL:
        raise RuntimeError("DATABASE_URL is required (PostgreSQL only mode)")
    if _PG_POOL is not None:
        return _PG_POOL

    last_exc: Exception | None = None
    for _ in range(30):
        try:
            pool = PgConnectionPool(
                conninfo=DATABASE_URL,
                min_size=DB_POOL_MIN_SIZE,
                max_size=DB_POOL_MAX_SIZE,
                open=True,
                kwargs={"row_factory": dict_row, "options": f"-c statement_timeout={DB_STATEMENT_TIMEOUT_MS}"},
            )
            with pool.connection() as conn:
                conn.execute("SELECT 1")
            _PG_POOL = pool
            return pool
        except Exception as exc:  # pragma: no cover - startup retry for container race
            last_exc = exc
            time.sleep(1.0)
    raise last_exc or RuntimeError("Failed to initialize PostgreSQL pool")


def reset_pg_pool() -> None:
    """Сбрасывает PostgreSQL-пул (BUG-15).

    Нужен после `psycopg.errors.OperationalError` / `AdminShutdown` — когда БД
    перезапустилась и старые соединения в пуле непригодны. Следующий вызов
    `_get_pg_pool()` создаст новый пул с рабочими соединениями.

    Безопасно вызывать повторно: при отсутствии пула — no-op.
    Закрытие старого пула делается best-effort (ошибки глушим и логируем),
    т.к. соединения уже могут быть битыми и close() тоже упадёт.
    """
    global _PG_POOL
    if _PG_POOL is None:
        return
    old_pool = _PG_POOL
    _PG_POOL = None
    try:
        old_pool.close()
    except Exception as exc:  # pragma: no cover - best effort cleanup
        logger.warning("Failed to close stale PG pool, will leak connections: %s", exc)


_SQL_TOKEN_RE = re.compile(
    r"""
    '(?:[^']|'')*'           # одинарные кавычки — литералы строк, с escape ''
    | "(?:[^"]|"")*"         # двойные кавычки — идентификаторы Postgres
    | --[^\n]*               # однострочный комментарий до конца строки
    | /\*.*?\*/              # многострочный комментарий
    | \?                     # плейсхолдер — единственный токен, который заменяем
    """,
    re.VERBOSE | re.DOTALL,
)


def _to_db_query(sql: str) -> str:
    """Заменяет `?`-плейсхолдеры на `%s`, пропуская литералы и комментарии (BUG-10).

    Раньше был наивный `sql.replace("?", "%s")` — ломал SQL где `?` встречался
    внутри строкового литерала, JSON, regex или комментария (`LIKE '%?%'`,
    `'{"q":"?"}'::jsonb` и т.п.).
    """

    def _replace(match: re.Match[str]) -> str:
        token = match.group(0)
        return "%s" if token == "?" else token

    return _SQL_TOKEN_RE.sub(_replace, sql)


class _ConnProxy:
    def __init__(self, conn: Any) -> None:
        self._conn = conn
        self._changes = 0
        self.cleanup_paths: list[Path] = []
        self.partial_invalidations: set[str] = set()

    def __getattr__(self, name: str) -> Any:
        return getattr(self._conn, name)

    @property
    def total_changes(self) -> int:
        if hasattr(self._conn, "total_changes"):
            return int(getattr(self._conn, "total_changes"))
        return self._changes

    def execute(self, sql: str, params: tuple[Any, ...] | list[Any] = ()) -> Any:
        cur = self._conn.execute(_to_db_query(sql), params)
        rowcount = getattr(cur, "rowcount", 0) or 0
        self._changes += max(0, int(rowcount))
        return cur

    def executemany(self, sql: str, seq_of_params: list[tuple[Any, ...]] | list[list[Any]]) -> Any:
        query = _to_db_query(sql)
        cur = self._conn.cursor()
        cur.executemany(query, seq_of_params)
        rowcount = getattr(cur, "rowcount", 0) or 0
        self._changes += max(0, int(rowcount))
        return cur


@contextmanager
def get_conn():
    pool = _get_pg_pool()
    with pool.connection() as raw_conn:
        conn = _ConnProxy(raw_conn)
        try:
            yield conn
            conn.commit()
            for report_id in conn.partial_invalidations:
                try:
                    from app.partial_exports import invalidate
                    invalidate(report_id)
                except Exception:
                    logger.warning('Очистка промежуточного файла отложена: report_id=%s', report_id)
            for path in conn.cleanup_paths:
                try:
                    path.unlink(missing_ok=True)
                except OSError as exc:
                    logger.warning("File cleanup failed after commit: %s: %s", path.name, exc)
        except Exception:
            # Явный rollback при исключении (BUG-08). Раньше полагались на то,
            # что psycopg_pool сам откатит транзакцию при возврате соединения —
            # это работало, но создавало скрытую зависимость от реализации пула.
            try:
                conn.rollback()
            except Exception:
                # Откат тоже может упасть (порвалось соединение, мёртвая транзакция
                # и т.п.); подавляем, чтобы не скрыть исходное исключение.
                pass
            raise


def init_db() -> None:
    _init_postgres_schema()
    maybe_maintain_llm_cache()


def _init_postgres_schema() -> None:
    with get_conn() as conn:
        conn.execute("SET LOCAL statement_timeout=0")
        conn.execute("SELECT pg_advisory_xact_lock(2026110801)")
        conn.execute("""
            CREATE TABLE IF NOT EXISTS ui_settings (
                singleton BOOLEAN PRIMARY KEY DEFAULT TRUE CHECK (singleton),
                language TEXT NOT NULL CHECK (language IN ('en', 'ru'))
            )
        """)
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                role TEXT NOT NULL DEFAULT 'user',
                password_hash TEXT,
                email TEXT,
                sso_subject TEXT,
                sso_provider TEXT,
                sso_login_name TEXT,
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user'")
        conn.execute("UPDATE users SET role = 'user' WHERE role IS NULL OR role = ''")
        # SSO поля (v2.1.0, IDEA-09)
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS email TEXT")
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS sso_subject TEXT")
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS sso_provider TEXT")
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS sso_login_name TEXT")
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS password_hash TEXT")
        conn.execute("ALTER TABLE users ALTER COLUMN password_hash DROP NOT NULL")
        conn.execute(
            "CREATE UNIQUE INDEX IF NOT EXISTS users_sso_subject_uidx "
            "ON users(sso_provider, sso_subject) WHERE sso_subject IS NOT NULL"
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS sessions (
                token TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                created_at TEXT NOT NULL,
                expires_at TEXT NOT NULL
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS uploaded_files (
                id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                original_name TEXT NOT NULL,
                path TEXT NOT NULL,
                created_at TEXT NOT NULL,
                inspect_status TEXT NOT NULL DEFAULT 'ready',
                inspect_sheets_json TEXT,
                inspect_suggested_sheet TEXT,
                inspect_suggested_column TEXT,
                inspect_error_text TEXT,
                inspect_updated_at TEXT
            )
            """
        )
        conn.execute("ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS inspect_status TEXT NOT NULL DEFAULT 'ready'")
        conn.execute("ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS inspect_sheets_json TEXT")
        conn.execute("ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS inspect_suggested_sheet TEXT")
        conn.execute("ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS inspect_suggested_column TEXT")
        conn.execute("ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS inspect_error_text TEXT")
        conn.execute("ALTER TABLE uploaded_files ADD COLUMN IF NOT EXISTS inspect_updated_at TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS reports (
                id TEXT NOT NULL,
                user_id BIGINT NOT NULL,
                job_id TEXT NOT NULL,
                status TEXT NOT NULL,
                provider TEXT,
                model TEXT,
                sheet_name TEXT,
                column_name TEXT,
                max_reviews INTEGER,
                parallelism INTEGER,
                temperature DOUBLE PRECISION DEFAULT 0,
                created_at TEXT NOT NULL,
                finished_at TEXT,
                updated_at TEXT,
                total_rows INTEGER DEFAULT 0,
                processed_rows INTEGER DEFAULT 0,
                progress_percent DOUBLE PRECISION DEFAULT 0,
                eta_seconds DOUBLE PRECISION,
                current_step TEXT,
                uploaded_file_id TEXT,
                prompt_template TEXT,
                include_raw_json INTEGER DEFAULT 1,
                api_key_encrypted TEXT,
                prompt_tokens BIGINT DEFAULT 0,
                completion_tokens BIGINT DEFAULT 0,
                total_tokens BIGINT DEFAULT 0,
                results_file TEXT,
                raw_file TEXT,
                summary_json TEXT,
                error_text TEXT,
                analysis_mode TEXT DEFAULT 'custom',
                output_schema_json TEXT,
                expected_json_template_json TEXT,
                input_columns_json TEXT,
                non_analysis_columns_json TEXT,
                group_by_column TEXT,
                group_max_rows INTEGER DEFAULT 100,
                use_cache INTEGER DEFAULT 1,
                PRIMARY KEY (id, user_id)
            ) PARTITION BY HASH (user_id)
            """
        )
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS group_by_column TEXT")
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS group_max_rows INTEGER DEFAULT 100")
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS non_analysis_columns_json TEXT")
        # Материализованные счётчики групп для отчётов с group_by_column.
        # Раньше /api/reports/active вычислял их через LEFT JOIN с COUNT(DISTINCT group_key)
        # по report_rows на каждый запрос — на 100 пользователях с polling 1.5 сек это
        # давало 67 RPS пересчёта по миллионам строк. Теперь worker:
        #   - producer ставит groups_total один раз при разбиении на группы;
        #   - worker инкрементирует groups_processed по мере завершения групп
        #     (атомарно с processed_rows под progress_lock).
        # Чтение — одна индексированная выборка по reports, без сканов report_rows.
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS groups_total INTEGER DEFAULT 0")
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS groups_processed INTEGER DEFAULT 0")
        # personal_queue=TRUE — отчёт в «личной очереди» юзера (превышен MAX_ACTIVE_PER_USER).
        # status='queued', но в Redis не пушится. На освобождении slot'а у юзера —
        # один такой отчёт промоутится (personal_queue → FALSE) и кладётся в общую очередь.
        # См. docs/DATABASE.md → reports.personal_queue.
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS personal_queue BOOLEAN NOT NULL DEFAULT FALSE")
        for definition in (
            "deleted_at TEXT", "deleted_by BIGINT", "delete_reason TEXT", "access_group_id BIGINT", "name TEXT"
        ):
            conn.execute(f"ALTER TABLE reports ADD COLUMN IF NOT EXISTS {definition}")
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reports_visible_user_created "
            "ON reports (user_id, created_at DESC) WHERE deleted_at IS NULL"
        )
        reports_partitions = _get_existing_hash_modulus(conn, "reports") or HASH_PARTITIONS
        for i in range(reports_partitions):
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS reports_p{i}
                PARTITION OF reports
                FOR VALUES WITH (MODULUS {reports_partitions}, REMAINDER {i})
                """
            )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS report_rows (
                report_id TEXT NOT NULL,
                row_number INTEGER NOT NULL,
                review_text TEXT,
                warnings_json TEXT,
                status TEXT NOT NULL,
                sentiment_label TEXT,
                negativity_score DOUBLE PRECISION,
                short_reason TEXT,
                key_topics_json TEXT,
                raw_response_json TEXT,
                input_json TEXT,
                passthrough_json TEXT,
                group_key TEXT,
                custom_json TEXT,
                error_text TEXT,
                prompt_tokens INTEGER DEFAULT 0,
                completion_tokens INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
                PRIMARY KEY (report_id, row_number)
            ) PARTITION BY HASH (report_id)
            """
        )
        report_rows_partitions = _get_existing_hash_modulus(conn, "report_rows") or HASH_PARTITIONS
        for i in range(report_rows_partitions):
            conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS report_rows_p{i}
                PARTITION OF report_rows
                FOR VALUES WITH (MODULUS {report_rows_partitions}, REMAINDER {i})
                """
            )
        conn.execute("ALTER TABLE report_rows ADD COLUMN IF NOT EXISTS group_key TEXT")
        conn.execute("ALTER TABLE report_rows ADD COLUMN IF NOT EXISTS passthrough_json TEXT")
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_cache (
                cache_key TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                prompt_hash TEXT NOT NULL,
                review_hash TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                raw_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                hits INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS llm_semantic_cache (
                semantic_key TEXT PRIMARY KEY,
                provider TEXT NOT NULL,
                model TEXT NOT NULL,
                analysis_mode TEXT NOT NULL,
                prompt_hash TEXT NOT NULL,
                expected_template_hash TEXT NOT NULL,
                output_schema_hash TEXT NOT NULL,
                embedding_json TEXT NOT NULL,
                analysis_json TEXT NOT NULL,
                raw_json TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                hits INTEGER DEFAULT 0
            )
            """
        )
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS user_presets (
                id TEXT PRIMARY KEY,
                user_id BIGINT NOT NULL,
                name TEXT NOT NULL,
                prompt_template TEXT NOT NULL,
                expected_json_template_json TEXT NOT NULL,
                template_hint TEXT,
                created_at TEXT NOT NULL,
                updated_at TEXT NOT NULL,
                UNIQUE(user_id, name)
            )
            """
        )
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_id ON reports (id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_job_id_user ON reports (job_id, user_id)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_reports_user_created_at ON reports (user_id, created_at DESC)")
        conn.execute("CREATE INDEX IF NOT EXISTS idx_report_rows_report_row_status ON report_rows (report_id, row_number, status)")
        # BUG-09: partial-индекс для режима группировки. Ускоряет list_pending_group_keys_batch
        # (GROUP BY group_key) и list_pending_rows_by_group_key (equality на всех трёх полях).
        # WHERE group_key IS NOT NULL — экономит место: строки вне группировки в индекс не попадают.
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_report_rows_group_key "
            "ON report_rows (report_id, group_key, status) "
            "WHERE group_key IS NOT NULL"
        )
        # Partial-индекс для быстрого SELECT FOR UPDATE SKIP LOCKED при promote
        # личной очереди юзера. Покрывает ровно нужный paths-pattern в
        # promote_personal_queue_to_shared() — индекс маленький (только записи
        # которые ждут своего слота).
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_reports_personal_queue "
            "ON reports (user_id, created_at) "
            "WHERE personal_queue = TRUE"
        )
        conn.execute(
            "CREATE INDEX IF NOT EXISTS idx_semantic_cache_lookup ON llm_semantic_cache (provider, model, analysis_mode, prompt_hash, expected_template_hash, output_schema_hash, updated_at DESC)"
        )
        for i in range(reports_partitions):
            conn.execute(
                f"ALTER TABLE reports_p{i} SET (autovacuum_vacuum_scale_factor = 0.02, autovacuum_analyze_scale_factor = 0.01, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_threshold = 500)"
            )
        for i in range(report_rows_partitions):
            conn.execute(
                f"ALTER TABLE report_rows_p{i} SET (autovacuum_vacuum_scale_factor = 0.01, autovacuum_analyze_scale_factor = 0.005, autovacuum_vacuum_threshold = 2000, autovacuum_analyze_threshold = 1000)"
            )
        conn.execute(
            "ALTER TABLE llm_cache SET (autovacuum_vacuum_scale_factor = 0.03, autovacuum_analyze_scale_factor = 0.01, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_threshold = 500)"
        )
        conn.execute(
            "ALTER TABLE llm_semantic_cache SET (autovacuum_vacuum_scale_factor = 0.03, autovacuum_analyze_scale_factor = 0.01, autovacuum_vacuum_threshold = 1000, autovacuum_analyze_threshold = 500)"
        )
        from app.access import init_schema
        init_schema(conn)
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS is_test_fixture BOOLEAN NOT NULL DEFAULT FALSE")
        from app.events import init_schema as init_events
        init_events(conn)
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS prompt_language TEXT NOT NULL DEFAULT 'ru'")
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS request_id TEXT")
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS success_rows BIGINT")
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS failed_rows BIGINT")
        conn.execute("ALTER TABLE reports ADD COLUMN IF NOT EXISTS limit_pause_count BIGINT NOT NULL DEFAULT 0")
        if not conn.execute("SELECT 1 FROM schema_migrations WHERE name='report_limit_counters_v2_2_0'").fetchone():
            conn.execute("""UPDATE reports r SET limit_pause_count=s.n FROM
                (SELECT report_id,COUNT(*) AS n FROM service_events WHERE event_type='report.limit' GROUP BY report_id) s
                WHERE r.id=s.report_id""")
            conn.execute("INSERT INTO schema_migrations(name,applied_at) VALUES ('report_limit_counters_v2_2_0',?)", (_now_iso(),))
        if not conn.execute("SELECT 1 FROM schema_migrations WHERE name='report_row_metrics_v2_2_0'").fetchone():
            # Исторические summary_json могут быть невалидными или содержать нецелые значения:
            # такие отчёты пропускаем (метрики останутся NULL = «неизвестно»), а не роняем старт.
            conn.execute(r"""UPDATE reports SET
                success_rows=CASE WHEN summary_json::jsonb->>'success_rows' ~ '^\d{1,18}$'
                    THEN (summary_json::jsonb->>'success_rows')::bigint END,
                failed_rows=CASE WHEN summary_json::jsonb->>'failed_rows' ~ '^\d{1,18}$'
                    THEN (summary_json::jsonb->>'failed_rows')::bigint END
                WHERE summary_json IS JSON OBJECT""")
            conn.execute("INSERT INTO schema_migrations(name,applied_at) VALUES ('report_row_metrics_v2_2_0',?)", (_now_iso(),))
        conn.execute(
            "INSERT INTO schema_migrations(name,applied_at) VALUES (?,?) ON CONFLICT(name) DO NOTHING",
            (SOFT_DELETE_MIGRATION, _now_iso()),
        )

        from app import model_catalog, user_profile, reporting_schema
        user_profile.init_schema(conn)
        model_catalog.init_schema(conn)
        reporting_schema.init_schema(conn)
        from app.partial_exports import init_schema as init_partial_exports
        init_partial_exports(conn)


def _get_existing_hash_modulus(conn: Any, parent_table: str) -> int | None:
    rows = conn.execute(
        """
        SELECT pg_get_expr(c.relpartbound, c.oid) AS bound
        FROM pg_class c
        JOIN pg_inherits i ON i.inhrelid = c.oid
        JOIN pg_class p ON p.oid = i.inhparent
        WHERE p.relname = ?
        LIMIT 1
        """,
        (parent_table,),
    ).fetchall()
    if not rows:
        return None
    bound = str(rows[0].get("bound") or "")
    match = re.search(r"modulus\s+(\d+)", bound, flags=re.IGNORECASE)
    if not match:
        return None
    try:
        value = int(match.group(1))
    except Exception:
        return None
    return value if value > 0 else None


def get_ui_language() -> str:
    with get_conn() as conn:
        row = conn.execute("SELECT language FROM ui_settings WHERE singleton").fetchone()
    return row["language"] if row else "en"


def set_ui_language(language: str) -> None:
    with get_conn() as conn:
        conn.execute(
            """INSERT INTO ui_settings (language) VALUES (?)
            ON CONFLICT (singleton) DO UPDATE SET language = EXCLUDED.language""",
            (language,),
        )


def create_user(
    username: str, password_hash: str, role: str = "user", *, join_guest: bool = False
) -> int | None:
    with get_conn() as conn:
        now = _now_iso()
        row = conn.execute(
            """INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?)
            ON CONFLICT (username) DO NOTHING RETURNING id""",
            (username, password_hash, role, now),
        ).fetchone()
        if row is None:
            return None
        user_id = int(row["id"])
        if join_guest:
            conn.execute(
                """INSERT INTO access_group_members (group_id, user_id, added_at)
                SELECT id, ?, ? FROM access_groups WHERE is_system AND is_active
                ON CONFLICT DO NOTHING""",
                (user_id, now),
            )
        return user_id


def update_user_password(username: str, password_hash: str) -> bool:
    with get_conn() as conn:
        result = conn.execute(
            "UPDATE users SET password_hash = ? WHERE username = ? AND sso_subject IS NULL",
            (password_hash, username),
        )
        return result.rowcount > 0


def get_user_by_username(username: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return dict(row) if row else None


def get_or_create_sso_user(
    issuer: str, subject: str, username: str, login_name: str | None, email: str | None
) -> dict[str, Any]:
    from app.access import audit
    from app.config import sso_admin_role
    from app.sso import SSOError

    pinned_admin = sso_admin_role(issuer, subject, login_name)
    with get_conn() as conn:
        conn.execute("SELECT pg_advisory_xact_lock(hashtextextended(?, 0))", (json.dumps([issuer, subject]),))
        existing = conn.execute(
            "SELECT id, username, role FROM users WHERE sso_provider=? AND sso_subject=? FOR UPDATE",
            (issuer, subject),
        ).fetchone()
        if existing:
            role = "admin" if pinned_admin else existing["role"]
            conn.execute(
                "UPDATE users SET sso_login_name=?, email=COALESCE(?, email), role=? WHERE id=?",
                (login_name, email, role, existing["id"]),
            )
            if role != existing["role"]:
                audit(conn, None, "user.role.bootstrap", "user", existing["id"],
                      {"role": existing["role"]}, {"role": role})
            return {**dict(existing), "role": role}

        role = "admin" if pinned_admin else "user"
        for number in range(1, 101):
            suffix = "" if number == 1 else f"#{number}"
            candidate = username[:64 - len(suffix)] + suffix
            row = conn.execute(
                """INSERT INTO users (username, role, email, sso_subject, sso_provider, sso_login_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?) ON CONFLICT (username) DO NOTHING RETURNING id, username, role""",
                (candidate, role, email, subject, issuer, login_name, _now_iso()),
            ).fetchone()
            if row:
                return dict(row)
    raise SSOError("Не удалось подобрать имя пользователя")


def create_session(user_id: int, days: int = 30) -> str:
    token = str(uuid.uuid4())
    now = datetime.now(timezone.utc)
    expires = now + timedelta(days=days)
    with get_conn() as conn:
        conn.execute(
            "INSERT INTO sessions (token, user_id, created_at, expires_at) VALUES (?, ?, ?, ?)",
            (token, user_id, now.isoformat(), expires.isoformat()),
        )
    return token


def delete_session(token: str) -> None:
    with get_conn() as conn:
        conn.execute("DELETE FROM sessions WHERE token = ?", (token,))


def get_session_user(token: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT u.*
            FROM sessions s
            JOIN users u ON u.id = s.user_id
            WHERE s.token = ? AND s.expires_at > ?
            """,
            (token, _now_iso()),
        ).fetchone()
    return dict(row) if row else None


def add_uploaded_file(
    file_id: str,
    user_id: int,
    original_name: str,
    path: str,
    *,
    inspect_status: str = "ready",
    sheets: list[dict[str, Any]] | None = None,
    suggested_sheet: str | None = None,
    suggested_column: str | None = None,
    inspect_error_text: str | None = None,
) -> None:
    sheets_json = json.dumps(sheets or [], ensure_ascii=False) if sheets is not None else None
    now = _now_iso()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO uploaded_files (
                id, user_id, original_name, path, created_at,
                inspect_status, inspect_sheets_json, inspect_suggested_sheet,
                inspect_suggested_column, inspect_error_text, inspect_updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                file_id,
                user_id,
                original_name,
                path,
                now,
                inspect_status,
                sheets_json,
                suggested_sheet,
                suggested_column,
                inspect_error_text,
                now,
            ),
        )


def get_uploaded_file(file_id: str, user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM uploaded_files WHERE id = ? AND user_id = ?",
            (file_id, user_id),
        ).fetchone()
    return dict(row) if row else None


def create_report(
    *,
    report_id: str,
    user_id: int,
    job_id: str,
    status: str,
    provider: str,
    model: str,
    sheet_name: str,
    max_reviews: int,
    parallelism: int,
    temperature: float,
    uploaded_file_id: str,
    prompt_template: str,
    include_raw_json: bool,
    api_key_encrypted: str | None,
    analysis_mode: str = "custom",
    output_schema: dict[str, Any] | None = None,
    expected_json_template: dict[str, Any] | None = None,
    input_columns: list[str] | None = None,
    non_analysis_columns: list[str] | None = None,
    group_by_column: str | None = None,
    group_max_rows: int = GROUP_MAX_ROWS,
    use_cache: bool = True,
    personal_queue: bool = False,
    access_group_id: int | None = None,
    name: str | None = None,
    prompt_language: str = "ru",
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO reports (
                id, user_id, job_id, status, provider, model, sheet_name,
                max_reviews, parallelism, temperature, created_at, updated_at, total_rows,
                processed_rows, progress_percent, eta_seconds, current_step,
                uploaded_file_id, prompt_template, include_raw_json, api_key_encrypted
                , prompt_tokens, completion_tokens, total_tokens, analysis_mode, output_schema_json, expected_json_template_json, input_columns_json
                , non_analysis_columns_json, group_by_column, group_max_rows, use_cache, personal_queue, prompt_language
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                report_id,
                user_id,
                job_id,
                status,
                provider,
                model,
                sheet_name,
                max_reviews,
                parallelism,
                float(temperature),
                _now_iso(),
                _now_iso(),
                0,
                0,
                0.0,
                None,
                "В очереди",
                uploaded_file_id,
                prompt_template,
                1 if include_raw_json else 0,
                api_key_encrypted,
                0,
                0,
                0,
                analysis_mode,
                json.dumps(output_schema, ensure_ascii=False) if output_schema is not None else None,
                json.dumps(expected_json_template, ensure_ascii=False) if expected_json_template is not None else None,
                json.dumps(input_columns or [], ensure_ascii=False),
                json.dumps(non_analysis_columns or [], ensure_ascii=False),
                group_by_column,
                int(group_max_rows or GROUP_MAX_ROWS),
                1 if use_cache else 0,
                bool(personal_queue),
                prompt_language,
            ),
        )
        if access_group_id is not None or name is not None:
            conn.execute("UPDATE reports SET access_group_id=?,name=? WHERE id=? AND user_id=?", (access_group_id, name, report_id, user_id))
        from app.logging_utils import current_context
        from app.events import record_event
        conn.execute("UPDATE reports SET request_id=? WHERE id=?", (current_context()["request_id"], report_id))
        conn.execute("UPDATE reports SET success_rows=0,failed_rows=0 WHERE id=?", (report_id,))
        record_event(conn, "report.created", "Отчёт создан и поставлен в очередь",
                     {"id": report_id, "job_id": job_id, "user_id": user_id, "access_group_id": access_group_id})


_TERMINAL_STEP_BY_STATUS: dict[str, str] = {
    "canceled": "Отменено",
    "failed": "Ошибка",
    "completed": "Завершено",
}


def _invalidate_partial_after_commit(conn, report_id):
    pending = getattr(conn, 'partial_invalidations', None)
    if isinstance(pending, set): pending.add(str(report_id))


def _bump_results_revision(conn, report_id):
    conn.execute('UPDATE reports SET results_revision=results_revision+1 WHERE id=?', (report_id,))


def reset_report_terminal_state(report_id: str) -> None:
    """Сбрасывает finished_at и error_text перед перезапуском отчёта.

    update_report_status использует COALESCE и не позволяет обнулить
    эти поля — поэтому для retry нужен отдельный прямой UPDATE.
    """
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE reports
            SET finished_at = NULL,
                results_revision = results_revision + 1,
                error_text = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (_now_iso(), report_id),
        )
        _invalidate_partial_after_commit(conn, report_id)



def reset_failed_and_skipped_rows(report_id: str) -> int:
    """Возвращает в `pending` все строки, которые не довели обработку:
    - `status='error'` (упали на модели/валидации)
    - `status='done'` с warning `skipped_large_group` в warnings_json
      (скипнутые по старому лимиту группы — пользователь хочет их полечить)

    Возвращает количество сброшенных строк. Использует один UPDATE с двумя
    условиями через OR, чтобы не делать два прохода.
    """
    with get_conn() as conn:
        cur = conn.execute(
            """
            UPDATE report_rows
            SET status = 'pending',
                custom_json = NULL,
                raw_response_json = NULL,
                error_text = NULL,
                warnings_json = NULL,
                prompt_tokens = 0,
                completion_tokens = 0,
                total_tokens = 0
            WHERE report_id = ?
              AND (
                status = 'error'
                OR (status = 'done' AND warnings_json LIKE ?)
              )
            """,
            (report_id, "%skipped_large_group%"),
        )
        _bump_results_revision(conn, report_id)
        _invalidate_partial_after_commit(conn, report_id)
        try:
            return int(cur.rowcount or 0)
        except Exception:
            return 0


def update_report_status(
    *,
    report_id: str,
    status: str,
    finished_at: str | None = None,
    results_file: str | None = None,
    raw_file: str | None = None,
    summary: dict[str, Any] | None = None,
    error_text: str | None = None,
    current_step: str | None = None,
) -> None:
    step_override = current_step if current_step is not None else _TERMINAL_STEP_BY_STATUS.get(status)
    # CTE prev — снимок старого статуса/user_id ДО UPDATE. Нужен чтобы понять
    # был ли это slot-freeing переход (active → non-active) — на нём авто-промоут
    # одного personal_queue отчёта юзера в общую очередь.
    with get_conn() as conn:
        row = conn.execute(
            """
            WITH prev AS (
                SELECT id, status AS old_status, personal_queue AS old_personal_queue, user_id
                FROM reports
                WHERE id = ? AND deleted_at IS NULL
            )
            UPDATE reports r
            SET status = ?,
                finished_at = COALESCE(?, finished_at),
                updated_at = ?,
                results_file = COALESCE(?, results_file),
                raw_file = COALESCE(?, raw_file),
                summary_json = COALESCE(?, summary_json),
                error_text = COALESCE(?, error_text),
                current_step = COALESCE(?, current_step)
            FROM prev
            WHERE r.id = prev.id
            RETURNING prev.old_status, prev.old_personal_queue, r.user_id, r.status AS new_status,
                r.id,r.job_id,r.access_group_id
            """,
            (
                report_id,
                status,
                finished_at,
                _now_iso(),
                results_file,
                raw_file,
                json.dumps(summary, ensure_ascii=False) if summary is not None else None,
                error_text,
                step_override,
            ),
        ).fetchone()

        if row and (status in {'completed','failed','canceled','queued'} or
                    (row['old_status']=='paused' and status=='running')):
            _invalidate_partial_after_commit(conn, report_id)
        if row and row["old_status"] != row["new_status"]:
            from app.events import record_event
            record_event(conn, "report." + status, {
                "running": "Начата обработка", "paused": "Обработка приостановлена",
                "queued": "Отчёт поставлен в очередь / продолжение", "canceled": "Обработка отменена",
                "completed": "Обработка завершена", "failed": "Ошибка обработки",
            }.get(status, status), row)
        if summary is not None:
            conn.execute("UPDATE reports SET success_rows=?,failed_rows=? WHERE id=?",
                         (summary.get("success_rows"), summary.get("failed_rows"), report_id))

    # Авто-промоут: если был active→non-active переход у отчёта который реально
    # занимал slot (old_status='queued' с personal_queue=TRUE НЕ занимал slot),
    # продвигаем один personal_queue отчёт юзера в общую очередь.
    # Намеренно вне транзакции conn — promote_personal_queue_to_shared
    # открывает свою.
    if row:
        d = dict(row)
        old_status = d.get("old_status")
        old_personal_queue = bool(d.get("old_personal_queue"))
        new_status = d.get("new_status")
        owner_user_id = d.get("user_id")
        was_occupying = (
            old_status in _SLOT_OCCUPYING_STATUSES
            and not old_personal_queue
        )
        if (
            was_occupying
            and new_status in _SLOT_FREEING_STATUSES
            and owner_user_id is not None
        ):
            try:
                try_promote_after_slot_freed(int(owner_user_id))
            except Exception as exc:  # pragma: no cover — best-effort, статус уже обновлён
                logger.warning(
                    "Auto-promote after status update failed (report_id=%s, %s→%s): %s",
                    report_id, old_status, new_status, exc,
                )


def update_report_progress(
    *,
    report_id: str,
    total_rows: int,
    processed_rows: int,
    progress_percent: float,
    eta_seconds: float | None,
    current_step: str,
    summary_json: str | None = None,
    groups_total: int | None = None,
    groups_processed: int | None = None,
) -> None:
    """Атомарно обновляет прогресс, summary_json и (опционально) счётчики групп.

    Все колонки пишутся в одной транзакции — фронту через polling приезжает
    согласованный snapshot: processed_rows / summary_json / groups_processed
    всегда одного возраста. Если пишем не-групповой отчёт — groups_total и
    groups_processed остаются 0 и в UPDATE не передаются.
    """
    set_clauses = [
        "total_rows = ?",
        "processed_rows = ?",
        "progress_percent = ?",
        "eta_seconds = ?",
        "current_step = ?",
        "updated_at = ?",
    ]
    params: list[Any] = [
        total_rows,
        processed_rows,
        progress_percent,
        eta_seconds,
        current_step,
        _now_iso(),
    ]
    if summary_json is not None:
        set_clauses.insert(-1, "summary_json = ?")
        params.insert(-1, summary_json)
        try:
            counts = json.loads(summary_json)
            for column in ("success_rows", "failed_rows"):
                if column in counts:
                    set_clauses.insert(-1, column + " = ?")
                    params.insert(-1, int(counts[column]))
        except (ValueError, TypeError):
            pass
    if groups_total is not None:
        set_clauses.insert(-1, "groups_total = ?")
        params.insert(-1, int(groups_total))
    if groups_processed is not None:
        set_clauses.insert(-1, "groups_processed = ?")
        params.insert(-1, int(groups_processed))
    # Версия меняется только при изменении данных, входящих в промежуточный файл.
    compared = [(total_rows,'total_rows'),(processed_rows,'processed_rows')]
    if groups_total is not None: compared.append((int(groups_total),'groups_total'))
    if groups_processed is not None: compared.append((int(groups_processed),'groups_processed'))
    if summary_json is not None:
        try:
            counts = json.loads(summary_json)
            compared.extend((int(counts[c]),c) for c in ('success_rows','failed_rows') if c in counts)
        except (ValueError,TypeError): pass
    set_clauses.append('results_revision=results_revision+CASE WHEN '+
        ' OR '.join(c+' IS DISTINCT FROM ?' for v,c in compared)+' THEN 1 ELSE 0 END')
    params.extend(v for v,c in compared)
    params.append(report_id)
    sql = f"UPDATE reports SET {', '.join(set_clauses)} WHERE id = ? AND deleted_at IS NULL"
    with get_conn() as conn:
        conn.execute(sql, tuple(params))


def get_report(report_id: str, user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT r.*," + _REPORT_QUOTA_COLUMNS + " FROM reports r " + _REPORT_QUOTA_JOIN +
            " WHERE r.id = ? AND r.user_id = ? AND r.deleted_at IS NULL",
            (report_id, user_id),
        ).fetchone()
    return dict(row) if row else None


def update_uploaded_file_inspect(
    file_id: str,
    user_id: int,
    *,
    inspect_status: str,
    sheets: list[dict[str, Any]] | None = None,
    suggested_sheet: str | None = None,
    suggested_column: str | None = None,
    inspect_error_text: str | None = None,
) -> None:
    sheets_json = json.dumps(sheets or [], ensure_ascii=False) if sheets is not None else None
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE uploaded_files
            SET inspect_status = ?,
                inspect_sheets_json = COALESCE(?, inspect_sheets_json),
                inspect_suggested_sheet = COALESCE(?, inspect_suggested_sheet),
                inspect_suggested_column = COALESCE(?, inspect_suggested_column),
                inspect_error_text = ?,
                inspect_updated_at = ?
            WHERE id = ? AND user_id = ?
            """,
            (
                inspect_status,
                sheets_json,
                suggested_sheet,
                suggested_column,
                inspect_error_text,
                _now_iso(),
                file_id,
                user_id,
            ),
        )


def get_report_any(report_id: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE id = ? AND deleted_at IS NULL LIMIT 1",
            (report_id,),
        ).fetchone()
    return dict(row) if row else None


def get_report_by_job_id(job_id: str, user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE job_id = ? AND user_id = ? AND deleted_at IS NULL",
            (job_id, user_id),
        ).fetchone()
    return dict(row) if row else None


def set_personal_queue(report_id: str, value: bool) -> None:
    """Прямо помечает отчёт `personal_queue=TRUE/FALSE`.

    Используется когда нужно перевести отчёт в личную очередь без изменения
    статуса (например при resume/retry: status='queued', но в Redis не
    пушим — кладём в personal_queue, ждём пока освободится slot).
    """
    with get_conn() as conn:
        conn.execute(
            "UPDATE reports SET personal_queue = ?, updated_at = ? WHERE id = ?",
            (bool(value), _now_iso(), report_id),
        )


def queue_paused_report(report_id: str, user_id: int, *, personal_queue: bool) -> bool:
    """Фиксирует queued до публикации payload; повторный resume не захватывает строку."""
    with get_conn() as conn:
        row = conn.execute("""UPDATE reports SET status='queued',personal_queue=?,current_step='В очереди',
            updated_at=?,finished_at=NULL,error_text=NULL WHERE id=? AND user_id=?
            AND deleted_at IS NULL AND status='paused' RETURNING id""",
            (personal_queue, _now_iso(), report_id, user_id)).fetchone()
        if row: _invalidate_partial_after_commit(conn, report_id)
    return row is not None


def count_active_slots(user_id: int) -> int:
    """Сколько отчётов юзера сейчас «занимают слот» в worker-пуле.

    Считаем running + queued (только в общей Redis-очереди, personal_queue=FALSE).
    paused НЕ считается (воркер при паузе освобождается, см. worker.py:288-292).
    Используется для решения «новый отчёт идёт в общую или в личную очередь?».
    """
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT COUNT(*) AS c
            FROM reports
            WHERE user_id = ?
              AND deleted_at IS NULL
              AND status IN ('running', 'queued')
              AND personal_queue = FALSE
            """,
            (user_id,),
        ).fetchone()
    return int(dict(row)["c"]) if row else 0


def promote_personal_queue_to_shared(user_id: int) -> dict[str, Any] | None:
    """Атомарно достаёт из личной очереди юзера один отчёт и возвращает его dict.

    Использует SELECT FOR UPDATE SKIP LOCKED LIMIT 1 — устойчиво к гонкам
    одновременных промоутов (например когда два running-отчёта одного юзера
    завершаются почти одновременно: каждый завершившийся вызывает promote,
    каждый забирает свой row).

    После UPDATE personal_queue=FALSE отчёт считается «в общей очереди».
    Caller обязан построить payload через build_job_payload_from_report() и
    запушить в Redis через enqueue_job(). Promote сам Redis не трогает —
    разделение ответственности.

    Возвращает dict отчёта или None если личная очередь пуста.
    """
    with get_conn() as conn:
        row = conn.execute(
            """
            UPDATE reports
            SET personal_queue = FALSE,
                updated_at = ?
            WHERE id = (
                SELECT id FROM reports
                WHERE user_id = ?
                  AND deleted_at IS NULL
                  AND status = 'queued'
                  AND personal_queue = TRUE
                ORDER BY created_at ASC
                LIMIT 1
                FOR UPDATE SKIP LOCKED
            )
              AND user_id = ?
            RETURNING *
            """,
            (_now_iso(), user_id, user_id),
        ).fetchone()
    return dict(row) if row else None


# Статусы, занимающие slot воркера. Только переход ИЗ одного из них в
# slot-freeing статус считается «slot освободился» — это триггер промоута
# одного personal_queue отчёта в общую очередь.
_SLOT_OCCUPYING_STATUSES = frozenset({"running", "queued"})

# Статусы, освобождающие slot воркера (active → non-active переход).
# При update_report_status в любой из них И old_status был в _SLOT_OCCUPYING_STATUSES —
# авто-промоут одного personal_queue отчёта юзера в общую очередь.
_SLOT_FREEING_STATUSES = frozenset({"paused", "canceled", "failed", "completed"})


def try_promote_after_slot_freed(user_id: int) -> str | None:
    """Промоутит один personal_queue отчёт юзера в общую очередь.

    Атомарный promote → построение payload → enqueue в Redis. Используется
    когда у юзера освободился slot (running/queued → paused/canceled/failed/
    completed). Если личная очередь пуста — no-op.

    Возвращает report_id промоутированного отчёта или None.

    Безопасно при несовпадении состояний (build_job_payload вернул error,
    enqueue_job вернул False как duplicate) — логируется warning и promote
    остаётся применённым (отчёт станет personal_queue=FALSE и подхватится
    recovery loop через интервал).
    """
    promoted = promote_personal_queue_to_shared(user_id)
    if not promoted:
        return None

    # Лениво импортим build_job_payload_from_report и enqueue_job:
    # — app.job_payloads импортит только app.config, циклов нет
    # — app.queue импортит app.db (get_redis_client) — циклический import
    #   на top-level сломает; lazy import чинит.
    from app.job_payloads import build_job_payload_from_report
    from app.queue import enqueue_job

    payload, err = build_job_payload_from_report(promoted)
    if err:
        logger.warning(
            "promote_personal_queue: build_job_payload failed for report_id=%s: %s",
            promoted.get("id"),
            err,
        )
        return None
    if not enqueue_job(payload):
        logger.warning(
            "promote_personal_queue: enqueue_job returned False for report_id=%s (duplicate marker?)",
            promoted.get("id"),
        )
        return None

    logger.info(
        "Personal queue promoted to shared: report_id=%s user_id=%s",
        promoted.get("id"),
        user_id,
    )
    return str(promoted.get("id"))


_REPORT_QUOTA_COLUMNS = """g.name AS access_group_name,g.is_active AS access_group_active,
    EXISTS(SELECT 1 FROM access_group_members gm WHERE gm.group_id=r.access_group_id AND gm.user_id=r.user_id)
    AS access_group_member"""
_REPORT_QUOTA_JOIN = " LEFT JOIN access_groups g ON g.id=r.access_group_id "
_REPORTS_LIST_SELECT = """
    SELECT
        r.*,
        uf.original_name AS source_original_name,
        r.groups_total     AS group_total,
        r.groups_processed AS group_processed,
""" + _REPORT_QUOTA_COLUMNS + """
    FROM reports r
    LEFT JOIN uploaded_files uf ON uf.id = r.uploaded_file_id
""" + _REPORT_QUOTA_JOIN


def list_reports(user_id: int, limit: int = 20) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            _REPORTS_LIST_SELECT
            + " WHERE r.user_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return _enrich_reports_with_tokens([dict(row) for row in rows])


def list_active_reports(user_id: int, limit: int = 20) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            _REPORTS_LIST_SELECT
            + """
            WHERE r.user_id = ?
              AND r.deleted_at IS NULL
              AND r.status IN ('running', 'queued', 'paused')
            ORDER BY r.created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    return _enrich_reports_with_tokens([dict(row) for row in rows])


def list_reports_by_user(admin_target_user_id: int, limit: int = 50) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            _REPORTS_LIST_SELECT
            + " WHERE r.user_id = ? AND r.deleted_at IS NULL ORDER BY r.created_at DESC LIMIT ?",
            (admin_target_user_id, limit),
        ).fetchall()
    return _enrich_reports_with_tokens([dict(row) for row in rows])


def list_users_admin(limit: int = 200) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                u.id,
                u.username,
                u.role,
                u.created_at,
                COALESCE(COUNT(DISTINCT r.id), 0) AS reports_count,
                MAX(s.created_at) AS last_login_at
            FROM users u
            LEFT JOIN reports r ON r.user_id = u.id
            LEFT JOIN sessions s ON s.user_id = u.id
            GROUP BY u.id, u.username, u.role, u.created_at
            ORDER BY u.created_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def admin_runtime_stats() -> dict[str, int]:
    with get_conn() as conn:
        # queued = только в общей очереди (то что реально занимает воркер-слоты).
        # Личная очередь (personal_queue=TRUE) показывается отдельным счётчиком,
        # чтобы админ понимал «3 в общей + 5 ждут своих слотов».
        queued = int(
            (
                conn.execute(
                    "SELECT COUNT(*) AS c FROM reports WHERE status = 'queued' AND personal_queue = FALSE"
                ).fetchone()
                or {}
            ).get("c", 0)
        )
        personal_queued = int(
            (
                conn.execute(
                    "SELECT COUNT(*) AS c FROM reports WHERE status = 'queued' AND personal_queue = TRUE"
                ).fetchone()
                or {}
            ).get("c", 0)
        )
        running = int(
            (
                conn.execute(
                    "SELECT COUNT(*) AS c FROM reports WHERE status = 'running'"
                ).fetchone()
                or {}
            ).get("c", 0)
        )
        paused = int(
            (
                conn.execute(
                    "SELECT COUNT(*) AS c FROM reports WHERE status = 'paused'"
                ).fetchone()
                or {}
            ).get("c", 0)
        )
        failed = int(
            (
                conn.execute(
                    "SELECT COUNT(*) AS c FROM reports WHERE status = 'failed'"
                ).fetchone()
                or {}
            ).get("c", 0)
        )
    return {
        "queued": queued,
        "personal_queued": personal_queued,
        "running": running,
        "paused": paused,
        "failed": failed,
    }


def list_recent_report_failures(limit: int = 30) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                r.id AS report_id,
                r.job_id,
                r.user_id,
                u.username,
                r.updated_at,
                r.error_text
            FROM reports r
            JOIN users u ON u.id = r.user_id
            WHERE r.status = 'failed'
              AND r.deleted_at IS NULL
              AND r.error_text IS NOT NULL
              AND r.error_text <> ''
            ORDER BY r.updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def list_inflight_reports() -> list[dict[str, Any]]:
    """Возвращает отчёты, которые должны быть «в обороте» воркер-пула.

    Используется recovery-loop'ом для обнаружения сирот (отчёты в running/queued
    без Redis-маркеров/lease'ов).

    Исключаем personal_queue=TRUE — они намеренно не в Redis, лежат в личной
    очереди юзера и ждут своего slot'а. Recovery их игнорирует, чтобы не
    пушить случайно в общую очередь раньше времени.
    """
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT * FROM reports
            WHERE deleted_at IS NULL AND status IN ('running', 'queued', 'paused')
              AND personal_queue = FALSE
            ORDER BY created_at ASC
            """
        ).fetchall()
    return [dict(row) for row in rows]


def cleanup_reports_keep_last_for_all_users(keep_last: int = 20) -> tuple[int, int]:
    autocancel_paused_reports()
    deleted_reports = 0
    skipped_active = 0
    with get_conn() as conn:
        users = conn.execute("SELECT DISTINCT user_id FROM reports WHERE deleted_at IS NULL").fetchall()
        for user_row in users:
            user_id = int(user_row.get("user_id") or 0)
            if user_id <= 0:
                continue
            deleted, skipped = _delete_stale_reports_for_user(conn, user_id=user_id, keep_last=keep_last)
            deleted_reports += deleted
            skipped_active += skipped
    return deleted_reports, skipped_active


def _unlink_after_commit(conn, path):
    paths = getattr(conn, "cleanup_paths", None)
    if isinstance(paths, list):
        paths.append(path)
    else:
        path.unlink(missing_ok=True)


def autocancel_paused_reports(ttl_days: int = PAUSED_REPORT_TTL_DAYS) -> int:
    if ttl_days <= 0:
        return 0
    cutoff = (datetime.now(timezone.utc) - timedelta(days=ttl_days)).isoformat()
    with get_conn() as conn:
        rows = conn.execute("""SELECT id,job_id FROM reports WHERE deleted_at IS NULL
            AND status='paused' AND updated_at<? FOR UPDATE SKIP LOCKED""", (cutoff,)).fetchall()
        from app.queue import has_running_lease
        canceled = 0
        for row in rows:
            if get_redis_client() is not None and has_running_lease(row["job_id"]):
                continue
            conn.execute("""UPDATE reports SET status='canceled',finished_at=?,updated_at=?,
                current_step=? WHERE id=? AND deleted_at IS NULL AND status='paused'""",
                (_now_iso(), _now_iso(), f"Отменено: срок паузы превысил {ttl_days} дн.", row["id"]))
            _invalidate_partial_after_commit(conn, str(row["id"]))
            canceled += 1
    return canceled


def _soft_delete_report(conn: Any, row: dict[str, Any], *, deleted_by: int | None, reason: str) -> bool:
    if row.get("deleted_at"):
        if row.get('id'): _invalidate_partial_after_commit(conn, str(row['id']))
        return True
    if row.get("status") in {"running", "queued", "paused"}:
        return False
    # Отмена может опередить завершение уже отправленного запроса.
    from app.queue import has_running_lease
    if get_redis_client() is not None and has_running_lease(str(row.get("job_id") or row["id"])):
        return False
    for field in ("results_file", "raw_file"):
        if row.get(field):
            _unlink_after_commit(conn, RESULTS_DIR / Path(row[field]).name)
    _invalidate_partial_after_commit(conn, str(row["id"]))
    conn.execute("DELETE FROM report_rows WHERE report_id = ?", (row["id"],))
    conn.execute(
        """UPDATE reports SET deleted_at = ?, deleted_by = ?, delete_reason = ?,
           api_key_encrypted = NULL, summary_json = NULL, results_file = NULL, raw_file = NULL
           WHERE id = ? AND user_id = ? AND deleted_at IS NULL""",
        (_now_iso(), deleted_by, reason, row["id"], row["user_id"]),
    )
    from app.events import record_event
    record_event(conn, "report.deleted", "Отчёт удалён; учтённый расход сохранён", row, actor_id=deleted_by)
    if row.get("uploaded_file_id"):
        _cleanup_orphan_uploads(conn, int(row["user_id"]), file_id=str(row["uploaded_file_id"]))
    if reason == "admin":
        from app.access import audit
        audit(conn, deleted_by, "report.delete", "report", row["id"],
              {"status": row["status"], "total_tokens": row.get("total_tokens", 0),
               "access_group_id": row.get("access_group_id")}, {"delete_reason": reason, "access_group_id": row.get("access_group_id")})
    _invalidate_usage_cache(str(row["id"]), int(row["user_id"]), row.get("access_group_id"))
    return True


def _invalidate_usage_cache(report_id: str, user_id: int | None = None, group_id: int | None = None,
                            *, token_delta: int = 0, created_at: str | None = None) -> None:
    client = get_redis_client()
    if client is not None:
        try:
            keys = [f"report_tokens:{report_id}"]
            if user_id is not None:
                keys.append(f"usage:{user_id}")
            client.delete(*keys)
            if group_id is not None:
                from app.quota import publish_usage
                publish_usage(client, group_id, user_id, created_at, token_delta)
        except Exception as exc:
            logger.warning("Usage cache invalidation failed: %s", exc)


def delete_report(report_id: str, user_id: int, *, deleted_by: int | None = None, reason: str = "user") -> bool:
    if reason not in {"user", "admin", "retention", "paused_ttl"}:
        raise ValueError("Неизвестная причина удаления")
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE id = ? AND user_id = ? FOR UPDATE",
            (report_id, user_id),
        ).fetchone()
        if not row:
            return False
        actor_id = user_id if reason == "user" and deleted_by is None else deleted_by
        return _soft_delete_report(conn, dict(row), deleted_by=actor_id, reason=reason)


def bulk_delete_reports(user_id: int, report_ids: list[str], *, admin_actor_id: int | None = None) -> dict[str, Any]:
    results = []
    for report_id in dict.fromkeys(report_ids):
        row = get_report(report_id, user_id)
        if row is None:
            results.append({"id": report_id, "status": "error", "code": "not_found", "message": "Отчёт не найден"})
            continue
        try:
            delete_options = {"deleted_by": admin_actor_id, "reason": "admin"} if admin_actor_id is not None else {}
            if row["status"] in {"queued", "running", "paused"} or not delete_report(report_id, user_id, **delete_options):
                results.append({"id": report_id, "status": "error", "code": "active_report", "message": "Нельзя удалить активный отчёт. Сначала отмените обработку."})
            else:
                results.append({"id": report_id, "status": "deleted"})
        except Exception:
            logger.exception("Bulk deletion failed for report_id=%s", report_id)
            results.append({"id": report_id, "status": "error", "code": "delete_failed", "message": "Не удалось удалить отчёт"})
    return {"results": results, "deleted": sum(r["status"] == "deleted" for r in results),
            "failed": sum(r["status"] == "error" for r in results)}


def _cleanup_orphan_uploads(conn: Any, user_id: int, *, file_id: str | None = None) -> None:
    files = conn.execute(
        "SELECT id, path FROM uploaded_files WHERE user_id = ?" + (" AND id = ?" if file_id else ""),
        (user_id, file_id) if file_id else (user_id,),
    ).fetchall()
    for file_row in files:
        ref_count = conn.execute(
            "SELECT COUNT(*) AS c FROM reports WHERE user_id = ? AND uploaded_file_id = ? AND deleted_at IS NULL",
            (user_id, file_row["id"]),
        ).fetchone()["c"]
        if int(ref_count or 0) > 0:
            continue

        path = file_row["path"]
        if path:
            _unlink_after_commit(conn, Path(path))
        conn.execute("DELETE FROM uploaded_files WHERE id = ? AND user_id = ?", (file_row["id"], user_id))


def cleanup_orphan_uploads_ttl(ttl_hours: int = UPLOAD_ORPHAN_TTL_HOURS) -> int:
    if int(ttl_hours) <= 0:
        return 0  # 0 = очистка orphan-xlsx отключена, висячие файлы живут вечно
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(ttl_hours)))
    cutoff_iso = cutoff.isoformat()
    removed = 0
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT uf.id, uf.user_id, uf.path
            FROM uploaded_files uf
            LEFT JOIN reports r ON r.user_id = uf.user_id AND r.uploaded_file_id = uf.id AND r.deleted_at IS NULL
            WHERE r.id IS NULL
              AND uf.created_at < ?
              AND COALESCE(uf.inspect_status, 'ready') NOT IN ('queued', 'parsing')
            """,
            (cutoff_iso,),
        ).fetchall()
        for row in rows:
            file_id = str(row.get("id") or "")
            user_id = int(row.get("user_id") or 0)
            path = str(row.get("path") or "")
            if path:
                _unlink_after_commit(conn, Path(path))
            conn.execute("DELETE FROM uploaded_files WHERE id = ? AND user_id = ?", (file_id, user_id))
            removed += 1
    return removed


def _delete_stale_reports_for_user(conn: Any, *, user_id: int, keep_last: int) -> tuple[int, int]:
    rows = conn.execute(
        "SELECT * FROM reports WHERE user_id = ? AND deleted_at IS NULL ORDER BY created_at DESC FOR UPDATE",
        (user_id,),
    ).fetchall()
    stale = rows[max(1, int(keep_last)) :]
    deleted = 0
    skipped_active = 0
    for row in stale:
        status = str(row.get("status") or "")
        if status in {"running", "queued", "paused"}:
            skipped_active += 1
            continue
        reason = "paused_ttl" if str(row.get("current_step") or "").startswith("Отменено: срок паузы") else "retention"
        if _soft_delete_report(conn, dict(row), deleted_by=None, reason=reason):
            deleted += 1
        else:
            skipped_active += 1
    return deleted, skipped_active


def _insert_placeholders_with_on_conflict(
    conn: Any,
    report_id: str,
    rows: list[dict[str, Any]],
    batch_size: int,
) -> int:
    """Fallback-путь для retry/recovery: INSERT ... ON CONFLICT DO NOTHING батчами.
    Медленнее COPY, но безопасно если уже есть placeholder'ы для отчёта."""
    inserted = 0
    for idx in range(0, len(rows), max(1, batch_size)):
        chunk = rows[idx : idx + max(1, batch_size)]
        values_sql: list[str] = []
        params: list[Any] = []
        for row in chunk:
            values_sql.append("(?, ?, ?, ?, ?, ?, ?, 'pending')")
            params.extend(
                [
                    report_id,
                    int(row["row_number"]),
                    row.get("review_text"),
                    row.get("input_json"),
                    row.get("passthrough_json"),
                    json.dumps(list(row.get("warnings") or []), ensure_ascii=False),
                    row.get("group_key"),
                ]
            )
        before = conn.total_changes
        conn.execute(
            f"""
            INSERT INTO report_rows (report_id, row_number, review_text, input_json, passthrough_json, warnings_json, group_key, status)
            VALUES {", ".join(values_sql)}
            ON CONFLICT(report_id, row_number) DO NOTHING
            """,
            params,
        )
        inserted += conn.total_changes - before
    return inserted


def _copy_placeholders(conn: Any, report_id: str, rows: list[dict[str, Any]]) -> int:
    """Быстрый путь для первого ingest: psycopg3 COPY FROM STDIN (v2.0.0, итерация 3.2).

    В 5-10× быстрее INSERT: PG обрабатывает данные потоком, без построчной
    валидации и без per-row WAL-записей. На 100k строк снижает пик CPU и
    WAL-давление в разы.

    ON CONFLICT COPY не поддерживает — отсюда вызывающий код проверяет что
    placeholder'ов для report_id ещё нет и только тогда попадает сюда.
    """
    # conn — _ConnProxy, реальный psycopg connection за _conn. cursor().copy()
    # — нативный psycopg3 API для потоковой записи в таблицу.
    raw_conn = getattr(conn, "_conn", conn)
    with raw_conn.cursor() as cur:
        with cur.copy(
            "COPY report_rows "
            "(report_id, row_number, review_text, input_json, passthrough_json, warnings_json, group_key, status) "
            "FROM STDIN"
        ) as copy:
            for row in rows:
                copy.write_row(
                    (
                        report_id,
                        int(row["row_number"]),
                        row.get("review_text"),
                        row.get("input_json"),
                        row.get("passthrough_json"),
                        json.dumps(list(row.get("warnings") or []), ensure_ascii=False),
                        row.get("group_key"),
                        "pending",
                    )
                )
    return len(rows)


def upsert_report_row_placeholders(
    *,
    report_id: str,
    rows: list[dict[str, Any]],
    batch_size: int = 2000,
    force_copy: bool = False,
) -> int:
    """Вставка плейсхолдеров report_rows для ingest xlsx.

    Два пути:
    - **Happy path (первый ingest):** COPY FROM STDIN — в 5-10× быстрее INSERT,
      меньше WAL, меньше CPU на обе стороны.
    - **Fallback (recovery/retry с уже существующими строками):** INSERT ...
      ON CONFLICT DO NOTHING батчами. COPY не поддерживает ON CONFLICT, а при
      повторной попытке ingest дубли row_number должны молча игнорироваться.

    Выбор пути:
    - `force_copy=True` — caller гарантирует что для report_id ещё ничего нет
      (например, _initialize_report_rows_streaming вызывается только когда
      count_report_rows вернул 0). Все батчи идут через COPY — критично для
      больших отчётов (100k+), иначе только первый батч в COPY, остальные
      падают в медленный INSERT, потому что после первого батча existing-check
      видит уже-вставленные строки.
    - По умолчанию — быстрая проверка «есть ли хоть одна строка для report_id»,
      и переключение на INSERT-fallback при наличии существующих row'ов.
    """
    if not rows:
        return 0
    with get_conn() as conn:
        if not force_copy:
            existing = conn.execute(
                "SELECT 1 FROM report_rows WHERE report_id = ? LIMIT 1",
                (report_id,),
            ).fetchone()
            if existing:
                inserted = _insert_placeholders_with_on_conflict(conn, report_id, rows, batch_size)
                if inserted: _bump_results_revision(conn, report_id)
                return inserted
        inserted = _copy_placeholders(conn, report_id, rows)
        if inserted: _bump_results_revision(conn, report_id)
        return inserted


def list_report_rows(report_id: str) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM report_rows WHERE report_id = ? ORDER BY row_number ASC",
            (report_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def iter_report_rows(
    report_id: str,
    *,
    batch_size: int = 2000,
    errors_only: bool = False,
    conn: Any = None,
) -> Iterator[dict[str, Any]]:
    """Стримит строки отчёта батчами через keyset pagination (v2.0.0, итерация 3.2).

    Раньше использовали OFFSET/LIMIT — на 100k строк это давало O(n²) сканов
    (batch 50 при OFFSET=98000 сканирует всю партицию чтобы отбросить 98k строк
    и отдать последние 2k). PG уходил в 100% CPU при открытии/выгрузке больших
    отчётов.

    Keyset (`WHERE row_number > last_seen`) опирается на индекс
    `idx_report_rows_report_row_status(report_id, row_number, ...)` — B-tree seek
    + последовательный scan страниц. Суммарно O(n) вместо O(n²).

    Не загружает весь отчёт в память — даже 700k строк отдаются по 2000 за раз.
    """
    last_row_number = -1
    while True:
        with (nullcontext(conn) if conn is not None else get_conn()) as connection:
            rows = connection.execute(
                f"""
                SELECT * FROM report_rows
                WHERE report_id = ? AND row_number > ?
                  {"AND status = 'error'" if errors_only else ""}
                ORDER BY row_number ASC
                LIMIT ?
                """,
                (report_id, last_row_number, batch_size),
            ).fetchall()
        if not rows:
            break
        for row in rows:
            yield dict(row)
        if len(rows) < batch_size:
            break
        last_row_number = int(rows[-1]["row_number"])




def get_report_summary_agg(report_id: str, *, conn: Any = None, include_groups: bool = False) -> dict[str, Any]:
    group_select = ("COUNT(DISTINCT group_key) AS groups_total, "
                    "COUNT(DISTINCT group_key) FILTER(WHERE status IN ('done','error')) AS groups_processed,"
                    if include_groups else "")
    with (nullcontext(conn) if conn is not None else get_conn()) as connection:
        row = connection.execute(
            f"""
            SELECT
                COUNT(*) AS total_rows,
                {group_select}
                SUM(CASE WHEN status IN ('done','error') THEN 1 ELSE 0 END) AS processed_rows,
                SUM(
                    CASE
                        WHEN COALESCE(error_text, '') = ''
                             AND custom_json IS NOT NULL
                        THEN 1 ELSE 0
                    END
                ) AS success_rows,
                SUM(CASE WHEN COALESCE(error_text, '') <> '' THEN 1 ELSE 0 END) AS failed_rows
            FROM report_rows
            WHERE report_id = ?
            """,
            (report_id,),
        ).fetchone()
    return dict(row) if row else {}


def _row_to_preview(
    row: dict[str, Any],
    *,
    group_by_column: str | None = None,
    override_row_number: int | None = None,
) -> dict[str, Any]:
    """Превращает строку `report_rows` в превью-DTO с динамическими колонками.

    В `columns` сливаются (в порядке приоритета): input_json → passthrough_json → custom_json.
    Вложенные dict в `custom_json` разворачиваются в ключи `"parent.child"`.

    Для группового режима (`group_by_column` задан) из input/passthrough остаётся
    только сама колонка группировки: остальные поля (текст отзыва, оценка, дата)
    относятся к одной конкретной строке группы и вводят пользователя в заблуждение —
    агрегат LLM описывает всю группу, а не эту строку.
    """
    def _safe_json(raw: Any) -> dict[str, Any]:
        if isinstance(raw, dict):
            return raw
        if isinstance(raw, str) and raw.strip():
            try:
                decoded = json.loads(raw)
                if isinstance(decoded, dict):
                    return decoded
            except Exception:
                return {}
        return {}

    def _flat(value: dict[str, Any], prefix: str = "") -> dict[str, Any]:
        out: dict[str, Any] = {}
        for key, val in value.items():
            full_key = f"{prefix}.{key}" if prefix else str(key)
            if isinstance(val, dict):
                out.update(_flat(val, full_key))
            else:
                out[full_key] = val
        return out

    input_data = _safe_json(row.get("input_json"))
    passthrough_data = _safe_json(row.get("passthrough_json"))
    columns: dict[str, Any] = {}
    if group_by_column:
        value = passthrough_data.get(group_by_column)
        if value is None:
            value = input_data.get(group_by_column)
        if value is None:
            value = row.get("group_key")
        if value is not None:
            columns[group_by_column] = value
    else:
        columns.update(input_data)
        columns.update(passthrough_data)
    columns.update(_flat(_safe_json(row.get("custom_json"))))

    warnings_raw = row.get("warnings") or row.get("warnings_json")
    warnings: list[str] = []
    if isinstance(warnings_raw, list):
        warnings = [str(w) for w in warnings_raw if w]
    elif isinstance(warnings_raw, str) and warnings_raw.strip():
        try:
            decoded = json.loads(warnings_raw)
            if isinstance(decoded, list):
                warnings = [str(w) for w in decoded if w]
            else:
                warnings = [warnings_raw]
        except Exception:
            warnings = [warnings_raw]

    return {
        "row_number": int(
            override_row_number
            if override_row_number is not None
            else (row.get("row_number") or 0)
        ),
        "columns": columns,
        "warnings": warnings,
        "error": row.get("error_text") or None,
    }


def build_report_analysis(
    report_id: str,
    *,
    preview_limit: int = 10,
) -> tuple[dict[str, Any], list[dict[str, Any]]]:
    """Строит упрощённую сводку (total/processed/success/failed) и превью первых N строк.

    Используется в эндпоинтах `/api/reports/{id}/analysis` и
    `/api/admin/reports/{id}/analysis` для страницы отчёта (итерация 1 отказа от
    обязательных полей: выпилен sentiment-блок и top-10, добавлено превью строк).
    """
    summary_raw = get_report_summary_agg(report_id)
    total_rows = int(summary_raw.get("total_rows") or 0)
    success_rows = int(summary_raw.get("success_rows") or 0)
    failed_rows = int(summary_raw.get("failed_rows") or 0)
    summary = {
        "total_rows": total_rows,
        "processed_rows": total_rows,
        "success_rows": success_rows,
        "failed_rows": failed_rows,
    }

    # Для групповых отчётов показываем по одной строке на группу — как в xlsx:
    # весь результат LLM одинаковый для строк внутри группы, дубли смысла не несут.
    with get_conn() as conn:
        report_row = conn.execute(
            "SELECT group_by_column FROM reports WHERE id = ?",
            (report_id,),
        ).fetchone()
    group_by_column = (
        str(report_row.get("group_by_column") or "").strip() if report_row else ""
    )
    is_grouped = bool(group_by_column)

    preview_rows: list[dict[str, Any]] = []
    seen_groups: set[str] = set()
    # Pending-строки (ещё не обработанные LLM) в превью не тащим — они пустые.
    # Фильтрация в Python: iter_report_rows уже стримит батчами по 2000, а нужно
    # всего preview_limit готовых строк, т.е. оверхед минимален.
    for row in iter_report_rows(report_id, batch_size=max(preview_limit * 4, 100)):
        status = str(row.get("status") or "")
        has_result = row.get("custom_json") is not None or row.get("analysis_json") is not None
        has_error = bool(row.get("error_text") or row.get("error"))
        if status not in {"done", "error"} and not has_result and not has_error:
            continue
        if is_grouped:
            group_key = str(row.get("group_key") or "")
            if group_key in seen_groups:
                continue
            seen_groups.add(group_key)
        preview_rows.append(
            _row_to_preview(
                row,
                group_by_column=group_by_column or None,
                # В групповом режиме показываем порядковый номер группы (1..N),
                # а не номер случайной строки-представителя из исходного файла.
                override_row_number=(len(preview_rows) + 1) if is_grouped else None,
            )
        )
        if len(preview_rows) >= preview_limit:
            break
    return summary, preview_rows


def list_pending_report_rows_batch(
    report_id: str,
    *,
    after_row_number: int = 0,
    limit: int = 2000,
) -> list[dict[str, Any]]:
    chunk = max(1, int(limit))
    after = max(0, int(after_row_number))
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM report_rows
            WHERE report_id = ?
              AND status = 'pending'
              AND row_number > ?
            ORDER BY row_number ASC
            LIMIT ?
            """,
            (report_id, after, chunk),
        ).fetchall()
    return [dict(row) for row in rows]


def get_report_groups_agg(report_id: str) -> dict[str, int]:
    """Возвращает {total_groups, processed_groups} одной агрегацией.

    Тяжёлая операция (COUNT DISTINCT по report_rows) — вызывать только один
    раз: при инициализации Job (recovery после рестарта worker'а или фикс
    осиротевшего отчёта). В горячем live-цикле не использовать — там
    счётчики держатся в памяти job.groups_total/groups_processed и пишутся
    в reports атомарно с processed_rows через update_report_progress.
    """
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(DISTINCT group_key) AS total_groups,
                COUNT(DISTINCT CASE WHEN status IN ('done', 'error') THEN group_key END) AS processed_groups
            FROM report_rows
            WHERE report_id = ?
              AND group_key IS NOT NULL
            """,
            (report_id,),
        ).fetchone()
    if not row:
        return {"total_groups": 0, "processed_groups": 0}
    return {
        "total_groups": int(row.get("total_groups") or 0),
        "processed_groups": int(row.get("processed_groups") or 0),
    }


def list_pending_group_keys_batch(
    report_id: str,
    *,
    limit: int = 500,
    after_first_row: int = 0,
) -> list[dict[str, Any]]:
    """Возвращает следующий батч pending-групп после `after_first_row`.

    Keyset-пагинация по first_row_number (MIN(row_number) внутри группы).
    Раньше использовался OFFSET, но producer + воркеры работают конкурентно:
    каждая обработанная группа выпадает из `status='pending'` ДО следующего
    запроса, и OFFSET пропускал ровно столько групп, сколько уже вышло из
    pending. На 8427 группах с batch=400 это давало потерю ~45% групп
    (см. baseline в v2.1.0). Keyset защищён от этого: `MIN(row_number) > X`
    отсекает уже выданные группы независимо от их текущего статуса.

    Аналогично row-producer'у в job_manager, который использует
    `after_row_number` через list_pending_report_rows_batch.
    """
    chunk = max(1, int(limit))
    after = max(0, int(after_first_row))
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT
                group_key,
                MIN(row_number) AS first_row_number,
                COUNT(*) AS group_rows
            FROM report_rows
            WHERE report_id = ?
              AND status = 'pending'
            GROUP BY group_key
            HAVING MIN(row_number) > ?
            ORDER BY MIN(row_number) ASC
            LIMIT ?
            """,
            (report_id, after, chunk),
        ).fetchall()
    return [dict(row) for row in rows]


def list_pending_rows_by_group_key(
    report_id: str,
    group_key: str,
) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT *
            FROM report_rows
            WHERE report_id = ?
              AND status = 'pending'
              AND group_key = ?
            ORDER BY row_number ASC
            """,
            (report_id, group_key),
        ).fetchall()
    return [dict(row) for row in rows]


def count_report_rows(report_id: str) -> tuple[int, int]:
    with get_conn() as conn:
        total = conn.execute(
            "SELECT COUNT(*) AS c FROM report_rows WHERE report_id = ?",
            (report_id,),
        ).fetchone()["c"]
        processed = conn.execute(
            "SELECT COUNT(*) AS c FROM report_rows WHERE report_id = ? AND status IN ('done', 'error')",
            (report_id,),
        ).fetchone()["c"]
    return int(total or 0), int(processed or 0)


BULK_UPDATE_BATCH_SIZE = 5000


def bulk_update_report_rows_same_result(
    *,
    report_id: str,
    row_numbers: list[int],
    sentiment_label: str | None,
    negativity_score: float | None,
    short_reason: str | None,
    category: str | None,
    raw_response: dict[str, Any] | None,
    error_text: str | None,
    custom_data: dict[str, Any] | None = None,
    total_prompt_tokens: int = 0,
    total_completion_tokens: int = 0,
    total_total_tokens: int = 0,
) -> None:
    """Batch-UPDATE для строк группы с ОДИНАКОВЫМ результатом (v2.0.0, итерация 3.2).

    Для группового режима LLM возвращает один ответ на всю группу, который применяется
    ко всем строкам. Раньше мы делали N отдельных UPDATE в цикле — на группе 20k это
    забивало PG на 100% CPU и визуально «зависало» прогресс. Теперь — батчами по 5000
    строк через `row_number = ANY(?)` — O(batches) вместо O(N) запросов.

    Токены (prompt/completion/total) суммарные по группе полностью записываются на
    ПЕРВУЮ строку группы, остальным — 0. При агрегации по отчёту итоговые цифры
    совпадут с фактическими LLM-вызовами.
    """
    if not row_numbers:
        return
    numbers = sorted(int(x) for x in row_numbers)
    status = "error" if error_text else "done"
    custom_json_str = (
        json.dumps(custom_data, ensure_ascii=False) if custom_data is not None else None
    )
    raw_json_str = (
        json.dumps(raw_response, ensure_ascii=False) if raw_response is not None else None
    )
    counter_row = None
    with get_conn() as conn:
        for start in range(0, len(numbers), BULK_UPDATE_BATCH_SIZE):
            chunk = numbers[start : start + BULK_UPDATE_BATCH_SIZE]
            conn.execute(
                """
                UPDATE report_rows
                SET status = ?,
                    sentiment_label = ?,
                    negativity_score = ?,
                    short_reason = ?,
                    custom_json = ?,
                    raw_response_json = ?,
                    error_text = ?,
                    prompt_tokens = 0,
                    completion_tokens = 0,
                    total_tokens = 0
                WHERE report_id = ? AND row_number = ANY(?)
                """,
                (
                    status,
                    sentiment_label,
                    negativity_score,
                    short_reason,
                    custom_json_str,
                    raw_json_str,
                    error_text,
                    report_id,
                    chunk,
                ),
            )

        if total_total_tokens or total_prompt_tokens or total_completion_tokens:
            first_row = numbers[0]
            # Читаем предыдущие значения токенов на первой строке группы — нужно
            # чтобы посчитать дельту для инкремента счётчика в reports. При retry
            # группа перезаписывается с новыми токенами, дельта учитывает разницу.
            prev = conn.execute(
                "SELECT prompt_tokens, completion_tokens, total_tokens "
                "FROM report_rows WHERE report_id = ? AND row_number = ?",
                (report_id, first_row),
            ).fetchone()
            prev_prompt = int(prev["prompt_tokens"] or 0) if prev else 0
            prev_completion = int(prev["completion_tokens"] or 0) if prev else 0
            prev_total = int(prev["total_tokens"] or 0) if prev else 0

            conn.execute(
                """
                UPDATE report_rows
                SET prompt_tokens = ?,
                    completion_tokens = ?,
                    total_tokens = ?
                WHERE report_id = ? AND row_number = ?
                """,
                (
                    int(total_prompt_tokens or 0),
                    int(total_completion_tokens or 0),
                    int(total_total_tokens or 0),
                    report_id,
                    first_row,
                ),
            )

            # Инкрементируем материализованные счётчики в reports — чтобы
            # get_user_usage и get_report_tokens_batch читали O(1) без SUM по
            # партиционированной report_rows (9M+ строк). Колонки tokens НЕ в
            # индексах reports → Postgres делает HOT update, индексы не трогаются.
            delta_prompt = int(total_prompt_tokens or 0) - prev_prompt
            delta_completion = int(total_completion_tokens or 0) - prev_completion
            delta_total = int(total_total_tokens or 0) - prev_total
            if delta_prompt or delta_completion or delta_total:
                counter_row = conn.execute(
                    """
                    UPDATE reports
                    SET results_revision = results_revision + 1,
                        prompt_tokens = COALESCE(prompt_tokens, 0) + ?,
                        completion_tokens = COALESCE(completion_tokens, 0) + ?,
                        total_tokens = COALESCE(total_tokens, 0) + ?,
                        updated_at = ?
                    WHERE id = ?
                    RETURNING user_id, access_group_id, created_at
                    """,
                    (delta_prompt, delta_completion, delta_total, _now_iso(), report_id),
                ).fetchone()
        if counter_row is None:
            _bump_results_revision(conn, report_id)
    if counter_row:
        _invalidate_usage_cache(report_id, int(counter_row["user_id"]), counter_row.get("access_group_id"),
                                token_delta=delta_total, created_at=counter_row.get("created_at"))


def update_report_row_result(
    *,
    report_id: str,
    row_number: int,
    sentiment_label: str | None,
    negativity_score: float | None,
    short_reason: str | None,
    category: str | None,
    raw_response: dict[str, Any] | None,
    error_text: str | None,
    custom_data: dict[str, Any] | None = None,
    prompt_tokens: int = 0,
    completion_tokens: int = 0,
    total_tokens: int = 0,
) -> None:
    status = "error" if error_text else "done"
    counter_row = None
    with get_conn() as conn:
        prev = conn.execute(
            "SELECT prompt_tokens, completion_tokens, total_tokens FROM report_rows WHERE report_id = ? AND row_number = ?",
            (report_id, row_number),
        ).fetchone()
        prev_prompt = int(prev["prompt_tokens"] or 0) if prev else 0
        prev_completion = int(prev["completion_tokens"] or 0) if prev else 0
        prev_total = int(prev["total_tokens"] or 0) if prev else 0

        conn.execute(
            """
            UPDATE report_rows
            SET status = ?,
                sentiment_label = ?,
                negativity_score = ?,
                short_reason = ?,
                key_topics_json = ?,
                raw_response_json = ?,
                custom_json = ?,
                error_text = ?,
                prompt_tokens = ?,
                completion_tokens = ?,
                total_tokens = ?
            WHERE report_id = ? AND row_number = ?
            """,
            (
                status,
                sentiment_label,
                negativity_score,
                short_reason,
                json.dumps([category] if isinstance(category, str) and category.strip() else [], ensure_ascii=False),
                json.dumps(raw_response, ensure_ascii=False) if raw_response is not None else None,
                json.dumps(custom_data, ensure_ascii=False) if custom_data is not None else None,
                error_text,
                int(prompt_tokens or 0),
                int(completion_tokens or 0),
                int(total_tokens or 0),
                report_id,
                row_number,
            ),
        )
        delta_prompt = int(prompt_tokens or 0) - prev_prompt
        delta_completion = int(completion_tokens or 0) - prev_completion
        delta_total = int(total_tokens or 0) - prev_total
        if delta_prompt or delta_completion or delta_total:
            counter_row = conn.execute(
                """
                UPDATE reports
                SET results_revision = results_revision + 1,
                        prompt_tokens = COALESCE(prompt_tokens, 0) + ?,
                    completion_tokens = COALESCE(completion_tokens, 0) + ?,
                    total_tokens = COALESCE(total_tokens, 0) + ?,
                    updated_at = ?
                WHERE id = ?
                RETURNING user_id, access_group_id, created_at
                """,
                (delta_prompt, delta_completion, delta_total, _now_iso(), report_id),
            ).fetchone()
        if error_text:
            from app.events import record_event
            from app.llm_errors import explain_error
            report = conn.execute("SELECT id,job_id,user_id,access_group_id FROM reports WHERE id=?", (report_id,)).fetchone()
            code = (raw_response or {}).get("code") or explain_error(Exception(error_text))["code"]
            record_event(conn, "model.error", f"Ошибка модели; строка {row_number}; код {code}", report)
        if counter_row is None:
            _bump_results_revision(conn, report_id)
    if counter_row:
        _invalidate_usage_cache(report_id, int(counter_row["user_id"]), counter_row.get("access_group_id"),
                                token_delta=delta_total, created_at=counter_row.get("created_at"))


USER_USAGE_CACHE_TTL_SEC = 300
USER_USAGE_CACHE_KEY = "usage:{user_id}"

REPORT_TOKENS_CACHE_TTL_SEC = 300
REPORT_TOKENS_CACHE_KEY = "report_tokens:{report_id}"


def get_user_usage(user_id: int) -> dict[str, int]:
    """Суммарное потребление токенов пользователя.

    История:
    - До v2.1.2 (BUG-22): SUM по `reports.tokens` — но поля никогда не заполнялись.
    - v2.1.2 (BUG-22 fix): SUM по `report_rows` JOIN с reports — корректно, но медленно.
    - v2.1.2 hotfix (BUG-26): Redis-кэш TTL=300 + USAGE_POLL_MS 20→60 сек —
      нагрузка ×15 меньше, но всё равно при cache miss SUM по 9M+ строк висел минутами.
    - **hotfix/token-counters (BUG-27 fix):** материализованные счётчики в `reports`.
      Воркер при каждой записи строк инкрементит `reports.{prompt,completion,total}_tokens`
      ([[update_report_row_result]] и [[bulk_update_report_rows_same_result]]).
      Здесь читаем O(1): SUM по 20-30 отчётам юзера в одной HASH-партиции по user_id.
      Redis-кэш оставлен как дополнительный слой (миллисекунды → микросекунды),
      но даже без него запрос отрабатывает за единицы мс.
    """
    cache_key = USER_USAGE_CACHE_KEY.format(user_id=user_id)
    redis_client = get_redis_client()
    if redis_client is not None:
        try:
            cached = redis_client.get(cache_key)
        except Exception:  # noqa: BLE001 — Redis-сбой не должен ломать UI
            cached = None
        if cached:
            try:
                data = json.loads(cached)
                return {
                    "prompt_tokens": int(data.get("prompt_tokens", 0)),
                    "completion_tokens": int(data.get("completion_tokens", 0)),
                    "total_tokens": int(data.get("total_tokens", 0)),
                }
            except (json.JSONDecodeError, TypeError, ValueError):
                pass

    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT
              COALESCE(SUM(prompt_tokens), 0) AS prompt_tokens,
              COALESCE(SUM(completion_tokens), 0) AS completion_tokens,
              COALESCE(SUM(total_tokens), 0) AS total_tokens
            FROM reports
            WHERE user_id = ?
            """,
            (user_id,),
        ).fetchone()
        result = {
            "prompt_tokens": int(row["prompt_tokens"] or 0),
            "completion_tokens": int(row["completion_tokens"] or 0),
            "total_tokens": int(row["total_tokens"] or 0),
        }

    if redis_client is not None:
        try:
            redis_client.setex(cache_key, USER_USAGE_CACHE_TTL_SEC, json.dumps(result))
        except Exception:  # noqa: BLE001 — кэш — это оптимизация, не источник правды
            pass

    return result


def get_report_tokens_batch(report_ids: list[str]) -> dict[str, dict[str, int]]:
    """Возвращает {report_id: {prompt_tokens, completion_tokens, total_tokens}} для списка отчётов.

    История:
    - v2.1.3: появилась как SUM по `report_rows` GROUP BY report_id + Redis-кэш TTL=300.
      На проде на 7 параллельных 1M-строчных отчётах SUM висел минутами, Postgres задыхался.
    - **hotfix/token-counters (BUG-27 fix):** читаем материализованные счётчики прямо из
      `reports` через index seek по pkey. Воркер обновляет их при каждой записи строк
      ([[update_report_row_result]] и [[bulk_update_report_rows_same_result]]).
      Запрос O(N отчётов в списке) вместо O(N строк в report_rows).
      Redis-кэш оставлен как дополнительный слой — необязателен, но снижает PG-нагрузку
      при частом обновлении списка одним юзером.
    """
    if not report_ids:
        return {}

    result: dict[str, dict[str, int]] = {}
    miss_ids: list[str] = []

    redis_client = get_redis_client()
    if redis_client is not None:
        keys = [REPORT_TOKENS_CACHE_KEY.format(report_id=rid) for rid in report_ids]
        try:
            cached_values = redis_client.mget(keys)
        except Exception:  # noqa: BLE001 — Redis-сбой не должен ломать UI
            cached_values = [None] * len(keys)
        for rid, raw in zip(report_ids, cached_values):
            if raw:
                try:
                    data = json.loads(raw)
                    result[rid] = {
                        "prompt_tokens": int(data.get("prompt_tokens", 0)),
                        "completion_tokens": int(data.get("completion_tokens", 0)),
                        "total_tokens": int(data.get("total_tokens", 0)),
                    }
                    continue
                except (json.JSONDecodeError, TypeError, ValueError):
                    pass
            miss_ids.append(rid)
    else:
        miss_ids = list(report_ids)

    if miss_ids:
        placeholders = ",".join(["?"] * len(miss_ids))
        with get_conn() as conn:
            rows = conn.execute(
                f"""
                SELECT id,
                       COALESCE(prompt_tokens, 0) AS prompt_tokens,
                       COALESCE(completion_tokens, 0) AS completion_tokens,
                       COALESCE(total_tokens, 0) AS total_tokens
                FROM reports
                WHERE id IN ({placeholders})
                """,
                miss_ids,
            ).fetchall()
        for row in rows:
            result[row["id"]] = {
                "prompt_tokens": int(row["prompt_tokens"] or 0),
                "completion_tokens": int(row["completion_tokens"] or 0),
                "total_tokens": int(row["total_tokens"] or 0),
            }

        if redis_client is not None:
            for rid in miss_ids:
                value = result.get(rid, {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0})
                try:
                    redis_client.setex(
                        REPORT_TOKENS_CACHE_KEY.format(report_id=rid),
                        REPORT_TOKENS_CACHE_TTL_SEC,
                        json.dumps(value),
                    )
                except Exception:  # noqa: BLE001 — кэш — это оптимизация
                    pass

    for rid in report_ids:
        if rid not in result:
            result[rid] = {"prompt_tokens": 0, "completion_tokens": 0, "total_tokens": 0}

    return result


def _enrich_reports_with_tokens(reports: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Подменяет мёртвые поля reports.{prompt,completion,total}_tokens на агрегаты из report_rows."""
    if not reports:
        return reports
    tokens_by_id = get_report_tokens_batch([r["id"] for r in reports])
    for r in reports:
        t = tokens_by_id.get(r["id"], {})
        r["prompt_tokens"] = t.get("prompt_tokens", 0)
        r["completion_tokens"] = t.get("completion_tokens", 0)
        r["total_tokens"] = t.get("total_tokens", 0)
    return reports


def list_user_presets(user_id: int, limit: int = 100) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM user_presets WHERE user_id = ? ORDER BY updated_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def upsert_user_preset(
    *,
    user_id: int,
    name: str,
    prompt_template: str,
    expected_json_template: dict[str, Any],
    template_hint: str | None,
) -> dict[str, Any]:
    now = _now_iso()
    preset_id = str(uuid.uuid4())
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO user_presets (
                id, user_id, name, prompt_template, expected_json_template_json, template_hint, created_at, updated_at
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(user_id, name) DO UPDATE SET
                prompt_template = excluded.prompt_template,
                expected_json_template_json = excluded.expected_json_template_json,
                template_hint = excluded.template_hint,
                updated_at = excluded.updated_at
            """,
            (
                preset_id,
                user_id,
                name.strip(),
                prompt_template,
                json.dumps(expected_json_template, ensure_ascii=False),
                template_hint,
                now,
                now,
            ),
        )
        row = conn.execute(
            "SELECT * FROM user_presets WHERE user_id = ? AND name = ?",
            (user_id, name.strip()),
        ).fetchone()
    return dict(row) if row else {}


def delete_user_preset(preset_id: str, user_id: int) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "DELETE FROM user_presets WHERE id = ? AND user_id = ?",
            (preset_id, user_id),
        )
        return cur.rowcount > 0


def get_cached_analysis(cache_key: str) -> dict[str, Any] | None:
    # L1 Redis-кэш убран в v2.1.2: писал ключи `llm_cache:*` без TTL и без
    # enforce-лимита, за неделю накапливал ~1M ключей (~1 ГБ) и валил Redis
    # OOM-kill'ом. Postgres-lookup по primary key cache_key — единицы
    # миллисекунд, на нашем профиле (≈1 RPS) разница для UX незаметна.
    with get_conn() as conn:
        row = conn.execute(
            "SELECT cache_key, analysis_json, raw_json FROM llm_cache WHERE cache_key = ?",
            (cache_key,),
        ).fetchone()
        if not row:
            return None
        conn.execute(
            "UPDATE llm_cache SET hits = COALESCE(hits, 0) + 1, updated_at = ? WHERE cache_key = ?",
            (_now_iso(), cache_key),
        )

    analysis = json.loads(row["analysis_json"])
    raw = json.loads(row["raw_json"]) if row["raw_json"] else None
    return {"analysis": analysis, "raw": raw}


def _cosine_similarity(left: list[float], right: list[float]) -> float:
    if not left or not right:
        return 0.0
    size = min(len(left), len(right))
    if size <= 0:
        return 0.0
    dot = 0.0
    left_norm = 0.0
    right_norm = 0.0
    for i in range(size):
        lv = float(left[i])
        rv = float(right[i])
        dot += lv * rv
        left_norm += lv * lv
        right_norm += rv * rv
    if left_norm <= 0 or right_norm <= 0:
        return 0.0
    return dot / (math.sqrt(left_norm) * math.sqrt(right_norm))


def find_semantic_cached_analysis(
    *,
    provider: str,
    model: str,
    analysis_mode: str,
    prompt_hash: str,
    expected_template_hash: str,
    output_schema_hash: str,
    embedding: list[float],
    threshold: float,
    candidates: int,
) -> dict[str, Any] | None:
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT semantic_key, embedding_json, analysis_json, raw_json, updated_at
            FROM llm_semantic_cache
            WHERE provider = ? AND model = ? AND analysis_mode = ?
              AND prompt_hash = ? AND expected_template_hash = ? AND output_schema_hash = ?
            ORDER BY updated_at DESC
            LIMIT ?
            """,
            (
                provider,
                model,
                analysis_mode,
                prompt_hash,
                expected_template_hash,
                output_schema_hash,
                max(1, candidates),
            ),
        ).fetchall()

        best_row: Any | None = None
        best_similarity = 0.0
        for row in rows:
            try:
                vector = json.loads(row["embedding_json"])
                if not isinstance(vector, list):
                    continue
                similarity = _cosine_similarity(
                    embedding,
                    [float(item) for item in vector if isinstance(item, (int, float))],
                )
            except Exception:
                continue
            if similarity > best_similarity:
                best_similarity = similarity
                best_row = row

        if best_row is None or best_similarity < threshold:
            return None

        conn.execute(
            "UPDATE llm_semantic_cache SET hits = COALESCE(hits, 0) + 1, updated_at = ? WHERE semantic_key = ?",
            (_now_iso(), best_row["semantic_key"]),
        )

    analysis = json.loads(best_row["analysis_json"])
    raw = json.loads(best_row["raw_json"]) if best_row["raw_json"] else None
    return {"analysis": analysis, "raw": raw, "similarity": round(best_similarity, 4)}


def _vacuum_db() -> None:
    if PgConnectionPool is None or not DATABASE_URL:
        return
    pool = _get_pg_pool()
    conn = pool.getconn()
    try:
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute("VACUUM (ANALYZE) llm_cache")
            cur.execute("VACUUM (ANALYZE) llm_semantic_cache")
    finally:
        conn.autocommit = False
        pool.putconn(conn)


def _enforce_llm_cache_limit(max_rows: int = MAX_LLM_CACHE_ROWS) -> int:
    with get_conn() as conn:
        count_row = conn.execute("SELECT COUNT(*) AS c FROM llm_cache").fetchone()
        current_rows = int(count_row["c"] or 0)
        if current_rows <= max_rows:
            return 0

        to_delete = current_rows - max_rows
        conn.execute(
            """
            DELETE FROM llm_cache
            WHERE cache_key IN (
                SELECT cache_key
                FROM llm_cache
                ORDER BY updated_at ASC
                LIMIT ?
            )
            """,
            (to_delete,),
        )
        return to_delete


def _enforce_semantic_cache_limit(max_rows: int = MAX_SEMANTIC_CACHE_ROWS) -> int:
    with get_conn() as conn:
        count_row = conn.execute("SELECT COUNT(*) AS c FROM llm_semantic_cache").fetchone()
        current_rows = int(count_row["c"] or 0)
        if current_rows <= max_rows:
            return 0

        to_delete = current_rows - max_rows
        conn.execute(
            """
            DELETE FROM llm_semantic_cache
            WHERE semantic_key IN (
                SELECT semantic_key
                FROM llm_semantic_cache
                ORDER BY updated_at ASC
                LIMIT ?
            )
            """,
            (to_delete,),
        )
        return to_delete


def maybe_maintain_llm_cache() -> None:
    global _last_cache_maintenance_ts
    now = time.time()
    if now - _last_cache_maintenance_ts < CACHE_MAINTENANCE_INTERVAL_SEC:
        return
    _last_cache_maintenance_ts = now

    removed = _enforce_llm_cache_limit(MAX_LLM_CACHE_ROWS)
    removed += _enforce_semantic_cache_limit(MAX_SEMANTIC_CACHE_ROWS)
    if removed > 0:
        _vacuum_db()


def put_cached_analysis(
    *,
    cache_key: str,
    provider: str,
    model: str,
    prompt_hash: str,
    review_hash: str,
    analysis: dict[str, Any],
    raw: dict[str, Any] | None,
) -> None:
    # L1 Redis-кэш убран в v2.1.2 (см. комментарий в get_cached_analysis).
    now = _now_iso()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO llm_cache (
                cache_key, provider, model, prompt_hash, review_hash, analysis_json, raw_json, created_at, updated_at, hits
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(cache_key) DO UPDATE SET
                analysis_json = excluded.analysis_json,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            """,
            (
                cache_key,
                provider,
                model,
                prompt_hash,
                review_hash,
                json.dumps(analysis, ensure_ascii=False),
                json.dumps(raw, ensure_ascii=False) if raw is not None else None,
                now,
                now,
            ),
        )
    maybe_maintain_llm_cache()


def delete_cached_analysis(cache_key: str) -> None:
    """Удалить запись из кэша по ключу. Используется при обнаружении битой записи
    (cache-hit прошёл валидацию ранее, но теперь не проходит — например, схема
    поменялась или кэш был записан в старой версии формата). После удаления
    следующий раз пойдём в модель реальным запросом."""
    # L1 Redis-кэш убран в v2.1.2 (см. комментарий в get_cached_analysis).
    with get_conn() as conn:
        conn.execute("DELETE FROM llm_cache WHERE cache_key = ?", (cache_key,))


def put_semantic_cached_analysis(
    *,
    provider: str,
    model: str,
    analysis_mode: str,
    prompt_hash: str,
    expected_template_hash: str,
    output_schema_hash: str,
    embedding: list[float],
    analysis: dict[str, Any],
    raw: dict[str, Any] | None,
) -> None:
    now = _now_iso()
    semantic_key = hashlib.sha256(
        (
            f"{provider}\n{model}\n{analysis_mode}\n{prompt_hash}\n"
            f"{expected_template_hash}\n{output_schema_hash}\n"
            f"{json.dumps(embedding, ensure_ascii=False)}"
        ).encode("utf-8")
    ).hexdigest()
    with get_conn() as conn:
        conn.execute(
            """
            INSERT INTO llm_semantic_cache (
                semantic_key, provider, model, analysis_mode, prompt_hash, expected_template_hash, output_schema_hash,
                embedding_json, analysis_json, raw_json, created_at, updated_at, hits
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
            ON CONFLICT(semantic_key) DO UPDATE SET
                analysis_json = excluded.analysis_json,
                raw_json = excluded.raw_json,
                updated_at = excluded.updated_at
            """,
            (
                semantic_key,
                provider,
                model,
                analysis_mode,
                prompt_hash,
                expected_template_hash,
                output_schema_hash,
                json.dumps(embedding, ensure_ascii=False),
                json.dumps(analysis, ensure_ascii=False),
                json.dumps(raw, ensure_ascii=False) if raw is not None else None,
                now,
                now,
            ),
        )
    maybe_maintain_llm_cache()
