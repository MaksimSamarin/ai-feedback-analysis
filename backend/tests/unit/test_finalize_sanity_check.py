"""Юнит-тесты на sanity-check перед финализацией и сброс ETA (v2.1.1).

Регрессия на два связанных бага:

1. Финализация (`_run_job` после worker-loop) безусловно ставила
   `processed_rows = job.total, progress_percent = 100, status = completed`,
   независимо от реального состояния `report_rows`. На фоне offset-бага
   в group-producer'е это давало «Готов» при 54.9% реально обработанных
   строк. Фикс: перед completed читаем `count_report_rows`, если
   `pending > 0` — `status=failed` с понятным текстом, processed
   пересчитывается из БД.

2. Cancel-ветка `_run_job` и `_handle_cancel_before_finalize` записывали
   в БД `eta_seconds = job.eta_seconds` — последнее живое значение.
   В UI это давало «Отменён · Осталось ~5 ч 35 мин». Фикс: при
   терминальных переходах ETA пишется ровно `0.0`.

Запуск:
    cd backend && pytest tests/unit/test_finalize_sanity_check.py -v
"""
from __future__ import annotations

from pathlib import Path


def _job_manager_source() -> str:
    """Читаем исходник job_manager.py напрямую — без import, чтобы тест
    не зависел от установленных рантайм-зависимостей (xlsxwriter и т.п.)."""
    here = Path(__file__).resolve()
    src_path = here.parent.parent.parent / "app" / "services" / "job_manager.py"
    assert src_path.exists(), f"Не нашли исходник: {src_path}"
    return src_path.read_text(encoding="utf-8")


def _extract_run_job(src: str) -> str:
    """Грубое извлечение тела _run_job по `async def _run_job`
    и следующему `async def`. Достаточно для строкового поиска паттернов."""
    start = src.find("async def _run_job(")
    assert start > 0, "Не нашли _run_job в исходнике"
    # Ищем следующую сигнатуру async def на том же уровне.
    next_async = src.find("\n    async def ", start + 1)
    return src[start: next_async if next_async > 0 else len(src)]


def _extract_handle_cancel(src: str) -> str:
    start = src.find("async def _handle_cancel_before_finalize(")
    assert start > 0, "Не нашли _handle_cancel_before_finalize в исходнике"
    next_def = src.find("\n    def ", start + 1)
    next_async = src.find("\n    async def ", start + 1)
    end_candidates = [x for x in (next_def, next_async) if x > 0]
    end = min(end_candidates) if end_candidates else len(src)
    return src[start:end]


def test_finalize_calls_count_report_rows_before_completed() -> None:
    """Перед `status=completed` есть проверка реального количества обработанных."""
    src = _extract_run_job(_job_manager_source())
    # Ищем именно вызов через to_thread — sanity-check присваивает результат в db_total/db_processed.
    sanity_pos = src.find("db_total, db_processed = await asyncio.to_thread(count_report_rows")
    # `job.status = JobStatus.completed` — момент когда отчёт помечается как готовый.
    completed_pos = src.find("job.status = JobStatus.completed")
    assert sanity_pos > 0, (
        "В _run_job должен быть sanity-вызов count_report_rows с присваиванием в db_total/db_processed"
    )
    assert completed_pos > 0, "В _run_job должен быть переход в JobStatus.completed"
    assert sanity_pos < completed_pos, (
        "Sanity-проверка через count_report_rows должна идти ДО блока завершения"
    )


def test_finalize_sets_failed_with_human_error_when_pending_remains() -> None:
    """В sanity-ветке: status=failed с понятным error_text для пользователя."""
    src = _extract_run_job(_job_manager_source())
    assert "JobStatus.failed" in src, "При недообработке ставим failed"
    assert "Нажмите" in src and "Повторить" in src, (
        "Текст ошибки должен подсказать юзеру нажать «Повторить»"
    )


def test_finalize_recomputes_processed_from_db_on_sanity_failure() -> None:
    """processed_rows в БД пересчитывается реальным значением из count_report_rows,
    не используется fake `job.total` (как в баге)."""
    src = _extract_run_job(_job_manager_source())
    assert "processed_rows=db_processed" in src, (
        "В sanity-ветке processed_rows должен браться из БД, не из job.total"
    )
    assert "total_rows=db_total" in src


def test_cancel_branch_zeroes_eta_in_db() -> None:
    """Cancel в _run_job: ETA в update_report_progress пишется ровно 0.0."""
    src = _extract_run_job(_job_manager_source())
    cancel_start = src.find("if job.cancel_event.is_set():")
    canceled_end = src.find("status=JobStatus.canceled.value", cancel_start)
    assert cancel_start > 0 and canceled_end > cancel_start
    cancel_block = src[cancel_start:canceled_end]
    assert "eta_seconds=0.0" in cancel_block, (
        "Cancel-ветка должна писать в БД eta=0.0 — иначе на UI остаётся "
        "«Отменён · Осталось ~N ч N мин»"
    )


def test_handle_cancel_before_finalize_zeroes_eta() -> None:
    """Аналогичная защита в _handle_cancel_before_finalize."""
    src = _extract_handle_cancel(_job_manager_source())
    assert "eta_seconds=0.0" in src, (
        "_handle_cancel_before_finalize тоже должен сбрасывать ETA в 0.0"
    )
    assert "job.eta_seconds = 0.0" in src, (
        "in-memory ETA тоже обнуляется (для согласованности с БД)"
    )


def test_canceled_progress_keeps_real_processed_not_total() -> None:
    """При cancel processed_rows = реальный job.processed, не total
    (отчёт отменён на середине — нельзя показывать «100%»)."""
    src = _extract_run_job(_job_manager_source())
    cancel_start = src.find("if job.cancel_event.is_set():")
    canceled_end = src.find("status=JobStatus.canceled.value", cancel_start)
    cancel_block = src[cancel_start:canceled_end]
    assert "processed_rows=job.processed" in cancel_block, (
        "Cancel пишет реальный processed, не total"
    )
