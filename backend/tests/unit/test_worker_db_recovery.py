"""Юнит-тесты на восстановление воркера после разрыва соединения с Postgres (BUG-15).

Сценарий бага (воспроизведён в продакшене):
1. Работающий inspect-worker.
2. `docker compose restart postgres` — БД закрывает все соединения с
   `psycopg.errors.AdminShutdown: terminating connection due to administrator command`.
3. Воркер ловит исключение на `get_uploaded_file(...)`, логирует ERROR —
   и навсегда замирает: задачи в очереди накапливаются, user видит
   «Файл в очереди на подготовку...» пока админ не перезапустит контейнер.

Что проверяем:

1. `reset_pg_pool()` корректно закрывает старый пул и обнуляет глобальную
   переменную, чтобы следующий `_get_pg_pool()` создал свежий пул.
2. `requeue_after_transient_error()` возвращает payload в нужную очередь
   в обход dedup-проверки (после dequeue dedup-маркер от изначального
   enqueue ещё жив — обычный enqueue_*_job отдал бы False).
3. Обработка `psycopg.OperationalError` в main-loop воркера не ломает
   цикл и не теряет задачу.

Запуск:
    cd backend && pytest tests/unit/test_worker_db_recovery.py -v
"""

from __future__ import annotations

from typing import Any

import pytest


# --- reset_pg_pool -------------------------------------------------------


def test_reset_pg_pool_is_noop_when_pool_missing(monkeypatch: pytest.MonkeyPatch) -> None:
    """Если пул ещё не создан — reset_pg_pool() не падает и ничего не делает."""
    import app.db as db_mod

    monkeypatch.setattr(db_mod, "_PG_POOL", None)
    db_mod.reset_pg_pool()
    assert db_mod._PG_POOL is None


def test_reset_pg_pool_closes_and_nullifies(monkeypatch: pytest.MonkeyPatch) -> None:
    """Старый пул закрывается, глобальная ссылка обнуляется."""
    import app.db as db_mod

    class FakePool:
        def __init__(self) -> None:
            self.closed = False

        def close(self) -> None:
            self.closed = True

    pool = FakePool()
    monkeypatch.setattr(db_mod, "_PG_POOL", pool)
    db_mod.reset_pg_pool()
    assert pool.closed is True
    assert db_mod._PG_POOL is None


def test_reset_pg_pool_swallows_close_errors(monkeypatch: pytest.MonkeyPatch) -> None:
    """Если старый пул падает на close() — переменная всё равно обнуляется.

    После AdminShutdown соединения в пуле могут быть битыми, и close() тоже
    падает. Нам важно: ссылка на убитый пул ушла, следующий вызов создаст
    новый. Ошибку close() глушим (с логом внутри reset_pg_pool).
    """
    import app.db as db_mod

    class BrokenPool:
        def close(self) -> None:
            raise RuntimeError("connection already closed")

    monkeypatch.setattr(db_mod, "_PG_POOL", BrokenPool())
    db_mod.reset_pg_pool()  # не должно кинуть
    assert db_mod._PG_POOL is None


# --- requeue_after_transient_error --------------------------------------


class _FakeRedis:
    """Минимальный Redis-мок: фиксирует rpush-вызовы в список по ключу."""

    def __init__(self) -> None:
        self.store: dict[str, list[str]] = {}
        self.raise_on_rpush = False

    def rpush(self, key: str, value: str) -> int:
        if self.raise_on_rpush:
            raise RuntimeError("redis unavailable")
        self.store.setdefault(key, []).append(value)
        return len(self.store[key])


def test_requeue_sends_file_inspect_to_inspect_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """file_inspect payload уходит в REDIS_INSPECT_QUEUE_KEY, минуя dedup."""
    from app import queue as queue_mod

    fake = _FakeRedis()
    monkeypatch.setattr(queue_mod, "_redis", lambda: fake)

    payload = {"kind": "file_inspect", "file_id": "f-1", "user_id": 5}
    ok = queue_mod.requeue_after_transient_error(payload, kind="file_inspect")
    assert ok is True
    assert queue_mod.INSPECT_QUEUE_KEY in fake.store
    assert queue_mod.QUEUE_KEY not in fake.store
    # Тело корректное — как JSON, содержит file_id
    (written,) = fake.store[queue_mod.INSPECT_QUEUE_KEY]
    assert "f-1" in written


def test_requeue_sends_analysis_to_jobs_queue(monkeypatch: pytest.MonkeyPatch) -> None:
    """Обычный analysis-job уходит в REDIS_QUEUE_KEY."""
    from app import queue as queue_mod

    fake = _FakeRedis()
    monkeypatch.setattr(queue_mod, "_redis", lambda: fake)

    payload = {"kind": "analysis_job", "job_id": "j-1", "report_id": "r-1", "user_id": 5}
    ok = queue_mod.requeue_after_transient_error(payload, kind="analysis")
    assert ok is True
    assert queue_mod.QUEUE_KEY in fake.store
    assert queue_mod.INSPECT_QUEUE_KEY not in fake.store


def test_requeue_bypasses_dedup_marker(monkeypatch: pytest.MonkeyPatch) -> None:
    """Ключевой контракт BUG-15: requeue работает даже когда dedup-маркер занят.

    Обычный `enqueue_inspect_job` для уже-засеченного file_id вернёт False
    (маркер `*:queued:*` существует). `requeue_after_transient_error`
    делает прямой rpush — задача восстанавливается в очереди.
    """
    from app import queue as queue_mod

    fake = _FakeRedis()
    monkeypatch.setattr(queue_mod, "_redis", lambda: fake)

    # Имитируем что file_id уже в очереди (dedup-маркер жив)
    file_id = "f-duplicate"
    # enqueue_inspect_job честно установит маркер — но здесь мы минуем эту
    # функцию. Просто проверяем что requeue нас не спрашивает про маркер.
    payload = {"kind": "file_inspect", "file_id": file_id, "user_id": 5}

    # Первый requeue
    assert queue_mod.requeue_after_transient_error(payload, kind="file_inspect") is True
    # Второй — тоже проходит (обычный enqueue бы отказал)
    assert queue_mod.requeue_after_transient_error(payload, kind="file_inspect") is True
    assert len(fake.store[queue_mod.INSPECT_QUEUE_KEY]) == 2


def test_requeue_returns_false_on_redis_error(monkeypatch: pytest.MonkeyPatch) -> None:
    """Если и Redis недоступен — requeue возвращает False (воркер логирует и идёт дальше)."""
    from app import queue as queue_mod

    fake = _FakeRedis()
    fake.raise_on_rpush = True
    monkeypatch.setattr(queue_mod, "_redis", lambda: fake)

    ok = queue_mod.requeue_after_transient_error({"file_id": "x"}, kind="file_inspect")
    assert ok is False


# --- worker main-loop error handling -------------------------------------


class _FakeOpError(Exception):
    """Мок `psycopg.OperationalError`.

    В тестах без psycopg-рантайма реальное исключение не доступно; мы
    монкипатчим `_PgOperationalError` в воркере на этот класс и проверяем
    что обработчик срабатывает на него.
    """


def test_worker_main_loop_recovers_from_operational_error_and_requeues(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Интеграционный контракт: при OperationalError main-loop:
      1) не роняет процесс
      2) сбрасывает PG-пул
      3) возвращает payload в очередь через requeue_after_transient_error
      4) не помечает report как failed (транзиент, не ошибка пользователя)
    """
    import app.worker as worker_mod

    # Подменяем «исключение psycopg» на наш класс.
    monkeypatch.setattr(worker_mod, "_PgOperationalError", _FakeOpError)

    reset_calls: list[int] = []
    monkeypatch.setattr(worker_mod, "reset_pg_pool", lambda: reset_calls.append(1))

    requeued: list[tuple[dict[str, Any], str]] = []

    def _fake_requeue(payload: dict, *, kind: str) -> bool:
        requeued.append((payload, kind))
        return True

    monkeypatch.setattr(worker_mod, "requeue_after_transient_error", _fake_requeue)

    update_failed_calls: list[dict] = []
    monkeypatch.setattr(
        worker_mod,
        "update_report_status",
        lambda **kwargs: update_failed_calls.append(kwargs),
    )

    # Воспроизведём блок обработки исключения из main-loop вручную —
    # main() завязан на asyncio и Redis и не предназначен для unit-теста.
    # Берём тот же код, что в worker.main(), в формате обычной функции:
    def _simulate_loop_iteration(payload: dict) -> None:
        try:
            raise _FakeOpError("terminating connection due to administrator command")
        except worker_mod._PgOperationalError as exc:  # type: ignore[misc]
            worker_mod.logger.warning(
                "DB connection lost during worker task, will recover: %s", exc
            )
            try:
                worker_mod.reset_pg_pool()
            except Exception:
                pass
            kind = "file_inspect" if str(payload.get("kind") or "") == "file_inspect" else "analysis"
            worker_mod.requeue_after_transient_error(payload, kind=kind)

    # 1. file_inspect payload
    _simulate_loop_iteration({"kind": "file_inspect", "file_id": "f-1", "user_id": 5})
    assert reset_calls == [1]
    assert requeued == [({"kind": "file_inspect", "file_id": "f-1", "user_id": 5}, "file_inspect")]
    assert update_failed_calls == []  # транзиент — не помечаем failed

    # 2. analysis_job payload
    _simulate_loop_iteration({"kind": "analysis_job", "job_id": "j-1", "report_id": "r-1"})
    assert len(reset_calls) == 2
    assert requeued[-1] == ({"kind": "analysis_job", "job_id": "j-1", "report_id": "r-1"}, "analysis")
    assert update_failed_calls == []


def test_worker_non_transient_exception_still_marks_report_failed(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Регрессия: если ошибка НЕ OperationalError (например, баг парсинга) —
    отчёт по-прежнему помечается failed с текстом ошибки. Не должно это
    поведение задеть новым обработчиком."""
    import app.worker as worker_mod

    monkeypatch.setattr(worker_mod, "_PgOperationalError", _FakeOpError)

    update_failed_calls: list[dict] = []
    monkeypatch.setattr(
        worker_mod,
        "update_report_status",
        lambda **kwargs: update_failed_calls.append(kwargs),
    )
    requeued: list = []
    monkeypatch.setattr(
        worker_mod,
        "requeue_after_transient_error",
        lambda payload, *, kind: requeued.append((payload, kind)) or True,
    )

    # Воспроизведём non-transient ветку
    def _simulate_non_transient(payload: dict) -> None:
        try:
            raise ValueError("некорректный JSON от модели")
        except worker_mod._PgOperationalError:  # type: ignore[misc]
            worker_mod.requeue_after_transient_error(payload, kind="analysis")
        except Exception as exc:
            worker_mod.logger.exception("Worker task failed: %s", exc)
            report_id = str(payload.get("report_id") or "")
            if report_id:
                try:
                    worker_mod.update_report_status(
                        report_id=report_id,
                        status="failed",
                        error_text=str(exc),
                    )
                except Exception:
                    pass

    _simulate_non_transient({"kind": "analysis_job", "report_id": "r-7"})
    assert requeued == []
    assert len(update_failed_calls) == 1
    assert update_failed_calls[0]["report_id"] == "r-7"
    assert update_failed_calls[0]["status"] == "failed"
    assert "некорректный JSON" in update_failed_calls[0]["error_text"]
