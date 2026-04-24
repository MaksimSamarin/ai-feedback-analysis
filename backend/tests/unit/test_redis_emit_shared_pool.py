"""Юнит-тесты на `Job.emit()` — использование общего Redis-пула (BUG-03).

После фикса BUG-03 `emit()`:
1. Берёт клиента через `get_redis_client()` (shared pool), а не создаёт через `Redis.from_url`
2. Выносит публикацию в `asyncio.to_thread` — не блокирует event loop
3. Проглатывает ошибки Redis — падение сети не должно рушить обработку отчёта

Запуск:
    cd backend && pytest tests/unit/test_redis_emit_shared_pool.py -v
"""

from __future__ import annotations

import asyncio
import types
from pathlib import Path
from unittest.mock import MagicMock

import pytest


def _make_stub_job(job_id: str = "test-job") -> types.SimpleNamespace:
    """Минимальный объект с полями, которые использует `Job.emit()`.

    Методу emit нужны только `self.id` и `self.event_queue` — даём SimpleNamespace
    вместо полноценного Job с 20+ обязательными полями.
    """
    return types.SimpleNamespace(
        id=job_id,
        event_queue=asyncio.Queue(),
    )


async def _call_emit(job_stub: types.SimpleNamespace, event_type: str = "progress",
                     payload: dict | None = None) -> None:
    """Вызываем Job.emit как unbound-метод на нашем стабе."""
    from app.services.job_manager import Job

    await Job.emit(job_stub, event_type, payload or {"processed": 1})


async def test_emit_reuses_shared_client(monkeypatch: pytest.MonkeyPatch) -> None:
    """100 emit-ов должны переиспользовать ОДИН объект клиента из get_redis_client."""
    fake_client = MagicMock(name="shared_redis_client")
    fake_client.publish = MagicMock(return_value=None)
    get_client_call_count = {"value": 0}

    def fake_get_client():
        get_client_call_count["value"] += 1
        return fake_client

    monkeypatch.setattr("app.services.job_manager.get_redis_client", fake_get_client)

    job = _make_stub_job()
    for _ in range(100):
        await _call_emit(job)

    assert get_client_call_count["value"] == 100, (
        "get_redis_client должен вызываться каждым emit (он возвращает кэшированный клиент)"
    )
    assert fake_client.publish.call_count == 100, (
        f"publish должен вызываться 100 раз, получено {fake_client.publish.call_count}"
    )


async def test_emit_never_calls_redis_from_url(monkeypatch: pytest.MonkeyPatch) -> None:
    """Регрессия: `Redis.from_url` не должен вызываться из emit — это старое поведение
    (новое TCP-соединение на каждый вызов)."""
    fake_client = MagicMock()
    monkeypatch.setattr("app.services.job_manager.get_redis_client", lambda: fake_client)

    # Ставим сигнализатор на Redis.from_url
    from_url_calls = {"count": 0}
    try:
        import redis as redis_module

        original_from_url = redis_module.Redis.from_url

        def spy_from_url(*args, **kwargs):
            from_url_calls["count"] += 1
            return original_from_url(*args, **kwargs)

        monkeypatch.setattr(redis_module.Redis, "from_url", spy_from_url)
    except ImportError:
        pytest.skip("redis не установлен")

    job = _make_stub_job()
    for _ in range(10):
        await _call_emit(job)

    assert from_url_calls["count"] == 0, (
        f"Redis.from_url был вызван {from_url_calls['count']} раз — регрессия BUG-03"
    )


async def test_emit_skips_if_client_is_none(monkeypatch: pytest.MonkeyPatch) -> None:
    """Если Redis недоступен (`get_redis_client` вернул None) — emit не падает,
    а событие всё равно кладётся в локальную `event_queue`."""
    monkeypatch.setattr("app.services.job_manager.get_redis_client", lambda: None)

    job = _make_stub_job()
    await _call_emit(job, event_type="status", payload={"state": "running"})

    # Событие должно быть в очереди, даже без Redis
    assert job.event_queue.qsize() == 1
    event = job.event_queue.get_nowait()
    assert event["type"] == "status"
    assert event["payload"] == {"state": "running"}


async def test_emit_swallows_publish_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Если `client.publish` бросил исключение (сеть пропала, Redis упал) —
    emit не должен поднимать ошибку: обработка отчёта не должна ломаться."""
    fake_client = MagicMock()
    fake_client.publish = MagicMock(side_effect=ConnectionError("redis down"))
    monkeypatch.setattr("app.services.job_manager.get_redis_client", lambda: fake_client)

    job = _make_stub_job()
    # Не должно быть exception
    await _call_emit(job)

    # Локальная очередь всё равно должна принять событие
    assert job.event_queue.qsize() == 1


async def test_emit_uses_to_thread_for_publish(monkeypatch: pytest.MonkeyPatch) -> None:
    """Публикация должна идти через `asyncio.to_thread` — чтобы синхронный redis-py
    не блокировал event loop. Мокаем `asyncio.to_thread` и проверяем что его позвали."""
    fake_client = MagicMock()
    fake_client.publish = MagicMock()
    monkeypatch.setattr("app.services.job_manager.get_redis_client", lambda: fake_client)

    to_thread_calls = {"count": 0, "args": []}
    real_to_thread = asyncio.to_thread

    async def spy_to_thread(func, *args, **kwargs):
        to_thread_calls["count"] += 1
        to_thread_calls["args"].append((func, args))
        return await real_to_thread(func, *args, **kwargs)

    monkeypatch.setattr("app.services.job_manager.asyncio.to_thread", spy_to_thread)

    job = _make_stub_job()
    await _call_emit(job)

    assert to_thread_calls["count"] == 1, (
        f"asyncio.to_thread должен вызываться ровно 1 раз за emit, "
        f"получено {to_thread_calls['count']}"
    )
    # В to_thread должна передаваться именно client.publish
    func_arg, _args = to_thread_calls["args"][0]
    assert func_arg is fake_client.publish


def test_job_manager_source_has_no_redis_from_url() -> None:
    """Регрессия на уровне исходников: `Redis.from_url` не должен фигурировать
    в `job_manager.py` — это признак создания нового соединения на каждый вызов."""
    src_path = Path(__file__).resolve().parents[2] / "app" / "services" / "job_manager.py"
    source = src_path.read_text(encoding="utf-8")

    assert "Redis.from_url" not in source, (
        "`Redis.from_url` появился в job_manager.py — регрессия BUG-03, "
        "каждый emit снова создаёт новое TCP-соединение."
    )
