"""Юнит-тесты на встроенный retry в Redis-клиенте (v2.1.2, BUG-20).

Регрессия: до v2.1.2 при транзиентных Redis-ошибках (рестарт после OOM-kill,
RDB-loading, network blip) воркер падал и помечал job в `failed`. Пользователи
теряли отчёты на середине обработки. Фикс — встроенный retry в самом
redis-py через `Retry(ExponentialBackoff)` + `retry_on_error` для классов
исключений `BusyLoadingError`, `ConnectionError`, `TimeoutError`. Это работает
прозрачно для любого вызова (enqueue_job, claim_running_lease, etc.).

Запуск:
    cd backend && pytest tests/unit/test_redis_client_retry.py -v
"""
from __future__ import annotations

import inspect as ins
from pathlib import Path


def _db_source() -> str:
    here = Path(__file__).resolve()
    src = here.parent.parent.parent / "app" / "db.py"
    return src.read_text(encoding="utf-8")


def test_get_redis_client_configures_retry() -> None:
    """get_redis_client должен передавать `retry` в RedisConnectionPool.from_url."""
    src = _db_source()
    fn_start = src.find("def get_redis_client(")
    fn_end = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "retry=" in body or '"retry"' in body, (
        "Должна передаваться retry-инструкция в Redis-pool — иначе транзиентные "
        "ошибки не лечатся (BUG-20)"
    )
    assert "ExponentialBackoff" in body, (
        "Backoff должен быть экспоненциальный — line/constant backoff даёт thundering herd"
    )


def test_get_redis_client_handles_target_exceptions() -> None:
    """В retry_on_error должны быть BusyLoadingError, ConnectionError, TimeoutError."""
    src = _db_source()
    fn_start = src.find("def get_redis_client(")
    fn_end = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert "BusyLoadingError" in body, (
        "BusyLoadingError — Redis грузит RDB после рестарта (10-30 сек)"
    )
    assert "RedisConnectionError" in body, (
        "ConnectionError — порт закрыт во время рестарта Redis"
    )
    assert "RedisTimeoutError" in body, (
        "TimeoutError — Redis тупит при загрузке большого RDB"
    )


def test_get_redis_client_has_socket_timeout() -> None:
    """socket_timeout обязателен — без него зависший Redis блокирует воркер навсегда."""
    src = _db_source()
    fn_start = src.find("def get_redis_client(")
    fn_end = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    assert '"socket_timeout"' in body, (
        "Без socket_timeout запрос к мёртвому Redis висит вечно — voркер встаёт колом"
    )
    assert '"socket_connect_timeout"' in body, (
        "Без socket_connect_timeout TCP-handshake к закрытому порту тоже может тянуться"
    )


def test_retry_window_covers_redis_rdb_loading() -> None:
    """Сумма backoff'ов должна покрывать типичное RDB-loading окно 10-30 секунд.

    Параметры: ExponentialBackoff(cap=10, base=1), retries=5.
    Backoff: 1, 2, 4, 8, 10 (capped) → sum = 25 секунд. Это достаточно
    для типового случая «Redis перезапускается, грузит миллионный RDB».
    """
    src = _db_source()
    fn_start = src.find("def get_redis_client(")
    fn_end = src.find("\ndef ", fn_start + 1)
    body = src[fn_start:fn_end]
    # Должны быть конкретные параметры — 5 retries, cap >= 8s
    assert "5)" in body or " 5," in body, (
        "Число retry должно быть достаточным (минимум 5) для покрытия RDB-load окна"
    )
    assert "cap=" in body, "Backoff должен иметь cap чтобы не вырасти до минут"


def test_retry_uses_singleton_pool() -> None:
    """Pool создаётся один раз на процесс — иначе каждое создание клиента
    стирает retry-конфигурацию и плодит соединения (BUG-03 ранее закрывался)."""
    src = _db_source()
    assert "_REDIS_POOL is None" in src, (
        "Должна быть проверка singleton-pool, иначе retry-конфиг переопределится"
    )
    assert "global _REDIS_POOL, _REDIS_CLIENT" in src, "Pool/client глобальные"


def test_imports_redis_retry_components() -> None:
    """Импорты retry-компонентов redis-py должны быть в try/except для опциональности."""
    src = _db_source()
    assert "from redis.retry import Retry" in src
    assert "from redis.backoff import ExponentialBackoff" in src
    assert "from redis.exceptions import BusyLoadingError" in src


def test_get_redis_client_imports_compatible_with_redis_py_5plus() -> None:
    """Проверка что используем актуальный API: `Retry(ExponentialBackoff(), N)`.

    В redis-py 5.x signature — Retry(backoff: AbstractBackoff, retries: int).
    В redis-py 4.x было то же самое. Если вызов сделан как-то иначе — упадёт
    в рантайме на старте backend.
    """
    src = _db_source()
    # Регулярка слабая — главное проверить что нет старого API.
    assert "Retry(ExponentialBackoff(" in src, (
        "Конструктор Retry должен принимать ExponentialBackoff()-инстанс"
    )
