"""Юнит-тесты на отложенную очистку `JobManager.jobs` (BUG-01).

После фикса BUG-01 завершённый Job удаляется из словаря `self.jobs` через
`JOB_CLEANUP_DELAY_SEC` секунд. Проверяем:
1. `_delayed_cleanup` действительно удаляет запись
2. Идемпотентность — повторный вызов не падает
3. `CancelledError` при shutdown не мешает освободить память
4. Обёртка `_run_job_with_cleanup` планирует cleanup и на успехе, и на падении
5. Значение JOB_CLEANUP_DELAY_SEC читается из env

Запуск:
    cd backend && pytest tests/unit/test_job_cleanup_after_finish.py -v
"""

from __future__ import annotations

import asyncio
import importlib
import os
import types
from unittest.mock import MagicMock

import pytest


def _make_manager():
    """Пустой JobManager для тестов cleanup-логики."""
    from app.services.job_manager import JobManager

    return JobManager()


def _fake_job(job_id: str = "test-job-1") -> types.SimpleNamespace:
    """Мини-объект с полем `id` — для _run_job_with_cleanup достаточно."""
    return types.SimpleNamespace(id=job_id)


async def test_delayed_cleanup_removes_job_from_dict() -> None:
    """`_delayed_cleanup(delay=0)` должен удалить запись из `self.jobs`."""
    manager = _make_manager()
    manager.jobs["job-xyz"] = MagicMock()

    await manager._delayed_cleanup("job-xyz", delay=0.0)

    assert "job-xyz" not in manager.jobs, (
        "_delayed_cleanup не удалил запись — BUG-01 регресс (память будет течь)"
    )


async def test_delayed_cleanup_is_idempotent() -> None:
    """Повторный вызов cleanup на уже отсутствующий ключ не должен падать."""
    manager = _make_manager()
    # Ключа нет изначально
    await manager._delayed_cleanup("missing-job", delay=0.0)
    # И ещё раз
    await manager._delayed_cleanup("missing-job", delay=0.0)
    # Никаких исключений — тест проходит


async def test_delayed_cleanup_handles_cancelled_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Если `asyncio.sleep` отменили (shutdown), запись всё равно должна удалиться."""
    manager = _make_manager()
    manager.jobs["job-cancelled"] = MagicMock()

    async def cancelled_sleep(_delay):
        raise asyncio.CancelledError()

    monkeypatch.setattr("app.services.job_manager.asyncio.sleep", cancelled_sleep)

    # Не должно пробрасываться — ловится внутри _delayed_cleanup
    await manager._delayed_cleanup("job-cancelled", delay=10.0)

    assert "job-cancelled" not in manager.jobs, (
        "После CancelledError cleanup всё равно должен освободить память"
    )


async def test_run_job_with_cleanup_schedules_cleanup_on_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """При успешном завершении `_run_job` планируется `_delayed_cleanup`."""
    manager = _make_manager()
    job = _fake_job("job-ok")
    manager.jobs[job.id] = job

    cleanup_calls: list[str] = []

    async def fake_run_job(_job, *, api_key):
        return None  # успешно

    async def fake_delayed_cleanup(job_id, delay=0.0):
        cleanup_calls.append(job_id)
        manager.jobs.pop(job_id, None)

    monkeypatch.setattr(manager, "_run_job", fake_run_job)
    monkeypatch.setattr(manager, "_delayed_cleanup", fake_delayed_cleanup)

    await manager._run_job_with_cleanup(job, api_key=None)
    # Даём шанс запланированному asyncio.create_task отработать
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert cleanup_calls == ["job-ok"], (
        f"_delayed_cleanup должен быть запланирован один раз для 'job-ok', получено {cleanup_calls}"
    )


async def test_run_job_with_cleanup_schedules_cleanup_on_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Даже если `_run_job` кинул исключение, cleanup должен быть запланирован
    (через `try/finally`)."""
    manager = _make_manager()
    job = _fake_job("job-fail")
    manager.jobs[job.id] = job

    cleanup_calls: list[str] = []

    async def fake_run_job(_job, *, api_key):
        raise RuntimeError("job blew up")

    async def fake_delayed_cleanup(job_id, delay=0.0):
        cleanup_calls.append(job_id)
        manager.jobs.pop(job_id, None)

    monkeypatch.setattr(manager, "_run_job", fake_run_job)
    monkeypatch.setattr(manager, "_delayed_cleanup", fake_delayed_cleanup)

    # Exception от _run_job должен проброситься наружу
    with pytest.raises(RuntimeError, match="job blew up"):
        await manager._run_job_with_cleanup(job, api_key=None)

    # Но cleanup всё равно запланирован
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    assert cleanup_calls == ["job-fail"], (
        "Даже при падении _run_job cleanup должен запуститься через finally — иначе регресс BUG-01"
    )


def test_cleanup_delay_reads_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    """`JOB_CLEANUP_DELAY_SEC` должен читаться из env при импорте модуля."""
    monkeypatch.setenv("JOB_CLEANUP_DELAY_SEC", "42")

    import app.services.job_manager as jm

    reloaded = importlib.reload(jm)
    assert reloaded.JOB_CLEANUP_DELAY_SEC == 42.0, (
        f"Ожидали JOB_CLEANUP_DELAY_SEC=42.0, получили {reloaded.JOB_CLEANUP_DELAY_SEC}"
    )


def test_cleanup_delay_has_sane_default(monkeypatch: pytest.MonkeyPatch) -> None:
    """Без env-переменной дефолт должен быть >= 0 (по договорённости — 300 секунд)."""
    monkeypatch.delenv("JOB_CLEANUP_DELAY_SEC", raising=False)

    import app.services.job_manager as jm

    reloaded = importlib.reload(jm)
    assert reloaded.JOB_CLEANUP_DELAY_SEC >= 0.0, (
        "Отрицательная задержка недопустима — проверьте что max(0.0, ...) не сломан"
    )
    # Проверяем что дефолт не тривиальный "0" — у нас договорились про 300
    assert reloaded.JOB_CLEANUP_DELAY_SEC == 300.0, (
        f"Дефолт JOB_CLEANUP_DELAY_SEC должен быть 300 (5 минут), "
        f"получено {reloaded.JOB_CLEANUP_DELAY_SEC}"
    )
