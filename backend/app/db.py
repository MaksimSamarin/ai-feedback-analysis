from __future__ import annotations

import json
import hashlib
import logging
import math
import os
import re
import time
import uuid
from contextlib import contextmanager
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
)
try:
    from redis import ConnectionPool as RedisConnectionPool, Redis
except Exception:  # pragma: no cover
    Redis = None  # type: ignore[assignment]
    RedisConnectionPool = None  # type: ignore[assignment]
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
_PG_POOL: PgConnectionPool | None = None
_REDIS_POOL: RedisConnectionPool | None = None
_REDIS_CLIENT: Redis | None = None


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


def get_redis_client() -> Redis | None:
    """Возвращает общий Redis-клиент с пулом соединений.

    Переиспользует один клиент на процесс — чтобы не создавать TCP-соединение
    при каждом вызове (см. BUG-03). Возвращает None если Redis недоступен.
    """
    global _REDIS_POOL, _REDIS_CLIENT
    if Redis is None or RedisConnectionPool is None:
        return None
    if not _REDIS_URL:
        return None
    try:
        if _REDIS_CLIENT is None:
            if _REDIS_POOL is None:
                _REDIS_POOL = RedisConnectionPool.from_url(_REDIS_URL, decode_responses=True)
            _REDIS_CLIENT = Redis(connection_pool=_REDIS_POOL)
        return _REDIS_CLIENT
    except Exception:
        return None


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
                kwargs={"row_factory": dict_row},
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
        conn.execute(
            """
            CREATE TABLE IF NOT EXISTS users (
                id BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
                username TEXT NOT NULL UNIQUE,
                password_hash TEXT NOT NULL,
                role TEXT NOT NULL DEFAULT 'user',
                created_at TEXT NOT NULL
            )
            """
        )
        conn.execute("ALTER TABLE users ADD COLUMN IF NOT EXISTS role TEXT NOT NULL DEFAULT 'user'")
        conn.execute("UPDATE users SET role = 'user' WHERE role IS NULL OR role = ''")
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
                prompt_tokens INTEGER DEFAULT 0,
                completion_tokens INTEGER DEFAULT 0,
                total_tokens INTEGER DEFAULT 0,
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


def create_user(username: str, password_hash: str, role: str = "user") -> int:
    with get_conn() as conn:
        row = conn.execute(
            "INSERT INTO users (username, password_hash, role, created_at) VALUES (?, ?, ?, ?) RETURNING id",
            (username, password_hash, (role or "user").strip() or "user", _now_iso()),
        ).fetchone()
        return int(row["id"])


def update_user_password(username: str, password_hash: str) -> bool:
    with get_conn() as conn:
        cur = conn.execute(
            "UPDATE users SET password_hash = ? WHERE username = ?",
            (password_hash, username),
        )
        return cur.rowcount > 0


def get_user_by_username(username: str) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE username = ?", (username,)).fetchone()
    return dict(row) if row else None


def get_user_by_id(user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute("SELECT * FROM users WHERE id = ?", (user_id,)).fetchone()
    return dict(row) if row else None


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
                , non_analysis_columns_json, group_by_column, group_max_rows, use_cache
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
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
            ),
        )


_TERMINAL_STEP_BY_STATUS: dict[str, str] = {
    "canceled": "Отменено",
    "failed": "Ошибка",
    "completed": "Завершено",
}


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
                error_text = NULL,
                updated_at = ?
            WHERE id = ?
            """,
            (_now_iso(), report_id),
        )


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
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE reports
            SET status = ?,
                finished_at = COALESCE(?, finished_at),
                updated_at = ?,
                results_file = COALESCE(?, results_file),
                raw_file = COALESCE(?, raw_file),
                summary_json = COALESCE(?, summary_json),
                error_text = COALESCE(?, error_text),
                current_step = COALESCE(?, current_step)
            WHERE id = ?
            """,
            (
                status,
                finished_at,
                _now_iso(),
                results_file,
                raw_file,
                json.dumps(summary, ensure_ascii=False) if summary is not None else None,
                error_text,
                step_override,
                report_id,
            ),
        )


def update_report_progress(
    *,
    report_id: str,
    total_rows: int,
    processed_rows: int,
    progress_percent: float,
    eta_seconds: float | None,
    current_step: str,
) -> None:
    with get_conn() as conn:
        conn.execute(
            """
            UPDATE reports
            SET total_rows = ?,
                processed_rows = ?,
                progress_percent = ?,
                eta_seconds = ?,
                current_step = ?,
                updated_at = ?
            WHERE id = ?
            """,
            (
                total_rows,
                processed_rows,
                progress_percent,
                eta_seconds,
                current_step,
                _now_iso(),
                report_id,
            ),
        )


def get_report(report_id: str, user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE id = ? AND user_id = ?",
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
            "SELECT * FROM reports WHERE id = ? LIMIT 1",
            (report_id,),
        ).fetchone()
    return dict(row) if row else None


def get_report_by_job_id(job_id: str, user_id: int) -> dict[str, Any] | None:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT * FROM reports WHERE job_id = ? AND user_id = ?",
            (job_id, user_id),
        ).fetchone()
    return dict(row) if row else None


_REPORTS_LIST_SELECT = """
    SELECT
        r.*,
        uf.original_name AS source_original_name,
        COALESCE(gs.total_groups, 0)     AS group_total,
        COALESCE(gs.processed_groups, 0) AS group_processed
    FROM reports r
    LEFT JOIN uploaded_files uf ON uf.id = r.uploaded_file_id
    LEFT JOIN (
        SELECT
            report_id,
            COUNT(DISTINCT group_key) AS total_groups,
            COUNT(DISTINCT CASE WHEN status IN ('done', 'error') THEN group_key END) AS processed_groups
        FROM report_rows
        WHERE group_key IS NOT NULL
        GROUP BY report_id
    ) gs ON gs.report_id = r.id
"""


def list_reports(user_id: int, limit: int = 20) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            _REPORTS_LIST_SELECT
            + " WHERE r.user_id = ? ORDER BY r.created_at DESC LIMIT ?",
            (user_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def list_active_reports(user_id: int, limit: int = 20) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            _REPORTS_LIST_SELECT
            + """
            WHERE r.user_id = ?
              AND r.status IN ('running', 'queued', 'paused')
            ORDER BY r.created_at DESC
            LIMIT ?
            """,
            (user_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


def list_reports_by_user(admin_target_user_id: int, limit: int = 50) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            _REPORTS_LIST_SELECT
            + " WHERE r.user_id = ? ORDER BY r.created_at DESC LIMIT ?",
            (admin_target_user_id, limit),
        ).fetchall()
    return [dict(row) for row in rows]


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
        queued = int(
            (
                conn.execute(
                    "SELECT COUNT(*) AS c FROM reports WHERE status = 'queued'"
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
              AND r.error_text IS NOT NULL
              AND r.error_text <> ''
            ORDER BY r.updated_at DESC
            LIMIT ?
            """,
            (limit,),
        ).fetchall()
    return [dict(row) for row in rows]


def list_inflight_reports() -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM reports WHERE status IN ('running', 'queued', 'paused') ORDER BY created_at ASC"
        ).fetchall()
    return [dict(row) for row in rows]


def cleanup_reports_keep_last_for_all_users(keep_last: int = 20) -> tuple[int, int]:
    deleted_reports = 0
    skipped_active = 0
    with get_conn() as conn:
        users = conn.execute("SELECT DISTINCT user_id FROM reports").fetchall()
        for user_row in users:
            user_id = int(user_row.get("user_id") or 0)
            if user_id <= 0:
                continue
            deleted, skipped = _delete_stale_reports_for_user(conn, user_id=user_id, keep_last=keep_last)
            deleted_reports += deleted
            skipped_active += skipped
    return deleted_reports, skipped_active


def delete_report(report_id: str, user_id: int) -> bool:
    with get_conn() as conn:
        row = conn.execute(
            "SELECT id, results_file, raw_file FROM reports WHERE id = ? AND user_id = ?",
            (report_id, user_id),
        ).fetchone()
        if not row:
            return False

        results_file = row["results_file"]
        raw_file = row["raw_file"]
        if results_file:
            (RESULTS_DIR / results_file).unlink(missing_ok=True)
        if raw_file:
            (RESULTS_DIR / raw_file).unlink(missing_ok=True)

        conn.execute("DELETE FROM report_rows WHERE report_id = ?", (report_id,))
        conn.execute("DELETE FROM reports WHERE id = ? AND user_id = ?", (report_id, user_id))
        _cleanup_orphan_uploads(conn, user_id)
        return True


def _cleanup_orphan_uploads(conn: Any, user_id: int) -> None:
    files = conn.execute(
        "SELECT id, path FROM uploaded_files WHERE user_id = ?",
        (user_id,),
    ).fetchall()
    for file_row in files:
        ref_count = conn.execute(
            "SELECT COUNT(*) AS c FROM reports WHERE user_id = ? AND uploaded_file_id = ?",
            (user_id, file_row["id"]),
        ).fetchone()["c"]
        if int(ref_count or 0) > 0:
            continue

        path = file_row["path"]
        if path:
            Path(path).unlink(missing_ok=True)
        conn.execute("DELETE FROM uploaded_files WHERE id = ? AND user_id = ?", (file_row["id"], user_id))


def cleanup_orphan_uploads_ttl(ttl_hours: int = UPLOAD_ORPHAN_TTL_HOURS) -> int:
    cutoff = datetime.now(timezone.utc) - timedelta(hours=max(1, int(ttl_hours)))
    cutoff_iso = cutoff.isoformat()
    removed = 0
    with get_conn() as conn:
        rows = conn.execute(
            """
            SELECT uf.id, uf.user_id, uf.path
            FROM uploaded_files uf
            LEFT JOIN reports r ON r.user_id = uf.user_id AND r.uploaded_file_id = uf.id
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
                Path(path).unlink(missing_ok=True)
            conn.execute("DELETE FROM uploaded_files WHERE id = ? AND user_id = ?", (file_id, user_id))
            removed += 1
    return removed


def _delete_stale_reports_for_user(conn: Any, *, user_id: int, keep_last: int) -> tuple[int, int]:
    rows = conn.execute(
        "SELECT id, results_file, raw_file, status FROM reports WHERE user_id = ? ORDER BY created_at DESC",
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
        results_file = row["results_file"]
        raw_file = row["raw_file"]
        if results_file:
            (RESULTS_DIR / results_file).unlink(missing_ok=True)
        if raw_file:
            (RESULTS_DIR / raw_file).unlink(missing_ok=True)
        conn.execute("DELETE FROM report_rows WHERE report_id = ?", (row["id"],))
        conn.execute("DELETE FROM reports WHERE id = ? AND user_id = ?", (row["id"], user_id))
        deleted += 1
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
) -> int:
    """Вставка плейсхолдеров report_rows для ingest xlsx.

    Два пути:
    - **Happy path (первый ingest):** COPY FROM STDIN — в 5-10× быстрее INSERT,
      меньше WAL, меньше CPU на обе стороны.
    - **Fallback (recovery/retry с уже существующими строками):** INSERT ...
      ON CONFLICT DO NOTHING батчами. COPY не поддерживает ON CONFLICT, а при
      повторной попытке ingest дубли row_number должны молча игнорироваться.

    Выбор пути — по быстрой проверке «есть ли хоть одна строка для report_id».
    """
    if not rows:
        return 0
    with get_conn() as conn:
        existing = conn.execute(
            "SELECT 1 FROM report_rows WHERE report_id = ? LIMIT 1",
            (report_id,),
        ).fetchone()
        if existing:
            return _insert_placeholders_with_on_conflict(conn, report_id, rows, batch_size)
        return _copy_placeholders(conn, report_id, rows)


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
        with get_conn() as conn:
            rows = conn.execute(
                """
                SELECT * FROM report_rows
                WHERE report_id = ? AND row_number > ?
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


def list_pending_report_rows(report_id: str) -> list[dict[str, Any]]:
    with get_conn() as conn:
        rows = conn.execute(
            "SELECT * FROM report_rows WHERE report_id = ? AND status = 'pending' ORDER BY row_number ASC",
            (report_id,),
        ).fetchall()
    return [dict(row) for row in rows]


def get_report_summary_agg(report_id: str) -> dict[str, Any]:
    with get_conn() as conn:
        row = conn.execute(
            """
            SELECT
                COUNT(*) AS total_rows,
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


def list_pending_group_keys_batch(
    report_id: str,
    *,
    limit: int = 500,
    offset: int = 0,
) -> list[dict[str, Any]]:
    chunk = max(1, int(limit))
    start = max(0, int(offset))
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
            ORDER BY MIN(row_number) ASC
            LIMIT ?
            OFFSET ?
            """,
            (report_id, chunk, start),
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
            conn.execute(
                """
                UPDATE reports
                SET prompt_tokens = COALESCE(prompt_tokens, 0) + ?,
                    completion_tokens = COALESCE(completion_tokens, 0) + ?,
                    total_tokens = COALESCE(total_tokens, 0) + ?,
                    updated_at = ?
                WHERE id = ?
                """,
                (delta_prompt, delta_completion, delta_total, _now_iso(), report_id),
            )


def get_user_usage(user_id: int) -> dict[str, int]:
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
        return {
            "prompt_tokens": int(row["prompt_tokens"] or 0),
            "completion_tokens": int(row["completion_tokens"] or 0),
            "total_tokens": int(row["total_tokens"] or 0),
        }


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
    redis_client = get_redis_client()
    if redis_client is not None:
        try:
            payload = redis_client.get(f"llm_cache:{cache_key}")
            if payload:
                parsed = json.loads(payload)
                if isinstance(parsed, dict):
                    return parsed
        except Exception:
            pass

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
    redis_client = get_redis_client()
    if redis_client is not None:
        try:
            redis_client.set(
                f"llm_cache:{cache_key}",
                json.dumps({"analysis": analysis, "raw": raw}, ensure_ascii=False),
            )
        except Exception:
            pass

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
    redis_client = get_redis_client()
    if redis_client is not None:
        try:
            redis_client.delete(f"llm_cache:{cache_key}")
        except Exception:
            pass
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
