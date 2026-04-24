from __future__ import annotations

import asyncio
import logging
import os
from datetime import datetime, timezone
from pathlib import Path

from app.config import (
    GROUP_MAX_ROWS,
    REPORT_CLEANUP_ENABLED,
    REPORT_CLEANUP_INTERVAL_SEC,
    REPORT_KEEP_LAST,
    UPLOAD_ORPHAN_TTL_HOURS,
    UPLOADS_DIR,
)
from app.crypto_utils import decrypt_text
from app.db import (
    cleanup_orphan_uploads_ttl,
    cleanup_reports_keep_last_for_all_users,
    get_report,
    get_uploaded_file,
    list_inflight_reports,
    reset_pg_pool,
    update_report_progress,
    update_report_status,
    update_uploaded_file_inspect,
)
from app.job_payloads import build_job_payload_from_report
from app.logging_utils import configure_logging
from app.queue import (
    acquire_lock,
    claim_running_lease,
    dequeue_inspect_job,
    dequeue_job,
    enqueue_job,
    get_queue_depth,
    has_queued_marker,
    has_running_lease,
    release_lock,
    release_running_lease,
    requeue_after_transient_error,
    touch_running_lease,
)
try:
    from psycopg import OperationalError as _PgOperationalError
except Exception:  # pragma: no cover
    _PgOperationalError = Exception  # type: ignore[assignment,misc]
from app.schemas import JobStatus
from app.services.excel_service import inspect_xlsx
from app.services.job_manager import job_manager

configure_logging("worker")
logger = logging.getLogger("review_analyzer.worker")

HEARTBEAT_INTERVAL_SEC = max(1, int(os.getenv("WORKER_HEARTBEAT_INTERVAL_SEC", "5")))
RECOVERY_INTERVAL_SEC = max(5, int(os.getenv("WORKER_RECOVERY_INTERVAL_SEC", "15")))
RUNNING_STALE_SEC = max(30, int(os.getenv("WORKER_RUNNING_STALE_SEC", "90")))
QUEUED_STALE_SEC = max(30, int(os.getenv("WORKER_QUEUED_STALE_SEC", "90")))
RUNNING_LEASE_TTL_SEC = max(15, int(os.getenv("WORKER_RUNNING_LEASE_TTL_SEC", "30")))
RECOVERY_LOCK_KEY = os.getenv("WORKER_RECOVERY_LOCK_KEY", "review_analyzer:worker:recovery_lock")
RECOVERY_LOCK_TTL_SEC = max(5, int(os.getenv("WORKER_RECOVERY_LOCK_TTL_SEC", "30")))
REPORT_CLEANUP_LOCK_KEY = os.getenv("WORKER_REPORT_CLEANUP_LOCK_KEY", "review_analyzer:worker:report_cleanup_lock")
REPORT_CLEANUP_LOCK_TTL_SEC = max(30, int(os.getenv("WORKER_REPORT_CLEANUP_LOCK_TTL_SEC", "300")))
WORKER_MODE = os.getenv("WORKER_MODE", "analysis").strip().lower()


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _parse_iso_ts(raw: object) -> datetime | None:
    text = str(raw or "").strip()
    if not text:
        return None
    try:
        # DB may store both "Z" and "+00:00" forms.
        return datetime.fromisoformat(text.replace("Z", "+00:00"))
    except Exception:
        return None


def _is_stale_running(row: dict) -> bool:
    updated = _parse_iso_ts(row.get("updated_at"))
    if updated is None:
        return True
    return (_now_utc() - updated).total_seconds() >= RUNNING_STALE_SEC


def _is_stale_queued(row: dict) -> bool:
    updated = _parse_iso_ts(row.get("updated_at"))
    if updated is None:
        return True
    return (_now_utc() - updated).total_seconds() >= QUEUED_STALE_SEC


def _recover_orphaned_reports_once() -> tuple[int, int]:
    token = acquire_lock(RECOVERY_LOCK_KEY, RECOVERY_LOCK_TTL_SEC)
    if not token:
        return 0, 0

    requeued = 0
    failed = 0
    try:
        queue_depth = get_queue_depth()
        for row in list_inflight_reports():
            status = str(row.get("status") or "")
            if status not in {JobStatus.running.value, JobStatus.queued.value}:
                continue

            job_id = str(row.get("job_id") or "")
            if status == JobStatus.running.value:
                if has_running_lease(job_id):
                    continue
                if not _is_stale_running(row):
                    continue
            else:
                # queued: no queue marker + no running lease for long time => orphaned queued job
                if has_queued_marker(job_id):
                    continue
                if has_running_lease(job_id):
                    continue
                # If queue is empty, recover immediately: queued row without marker/lease
                # cannot be picked by any worker and will stall forever otherwise.
                if queue_depth > 0 and not _is_stale_queued(row):
                    continue

            report_id = str(row.get("id") or "")
            payload, err = build_job_payload_from_report(row)
            if err:
                update_report_status(
                    report_id=report_id,
                    status=JobStatus.failed.value,
                    finished_at=_now_utc().isoformat(),
                    error_text=err,
                )
                failed += 1
                continue

            if enqueue_job(payload):
                update_report_status(report_id=report_id, status=JobStatus.queued.value)
                requeued += 1
                logger.warning("Recovered stale inflight report: status=%s report_id=%s job_id=%s", status, report_id, job_id)
    finally:
        release_lock(RECOVERY_LOCK_KEY, token)
    return requeued, failed


def _run_report_cleanup_once() -> tuple[int, int, int]:
    token = acquire_lock(REPORT_CLEANUP_LOCK_KEY, REPORT_CLEANUP_LOCK_TTL_SEC)
    if not token:
        return 0, 0, 0
    try:
        deleted_reports, skipped_active = cleanup_reports_keep_last_for_all_users(keep_last=REPORT_KEEP_LAST)
        removed_uploads = cleanup_orphan_uploads_ttl(ttl_hours=UPLOAD_ORPHAN_TTL_HOURS)
        return deleted_reports, skipped_active, removed_uploads
    finally:
        release_lock(REPORT_CLEANUP_LOCK_KEY, token)


def _heartbeat(job) -> None:
    total = max(0, int(job.total or 0))
    processed = max(0, int(job.processed or 0))
    progress = (float(processed) * 100.0 / float(total)) if total > 0 else 0.0
    progress = min(100.0, max(0.0, progress))
    step = str(job.current_step or f"Обработка {processed}/{total}")
    update_report_progress(
        report_id=job.report_id,
        total_rows=total,
        processed_rows=processed,
        progress_percent=progress,
        eta_seconds=job.eta_seconds,
        current_step=step,
    )


async def _run_payload(payload: dict) -> None:
    payload_kind = str(payload.get("kind") or "analysis_job").strip().lower()
    if payload_kind == "file_inspect":
        await _run_file_inspect_payload(payload)
        return

    report_id = str(payload.get("report_id") or "")
    job_id = str(payload.get("job_id") or "")
    user_id = int(payload.get("user_id") or 0)
    if not report_id or not job_id or not user_id:
        return

    report = get_report(report_id, user_id)
    if not report:
        return
    current_status = str(report.get("status") or "")
    if current_status in {JobStatus.canceled.value, JobStatus.completed.value, JobStatus.failed.value}:
        return

    # If job is paused before worker starts processing, skip payload.
    # Resume endpoint will enqueue a fresh payload when user clicks "Старт".
    if current_status == JobStatus.paused.value:
        return

    if not claim_running_lease(job_id, RUNNING_LEASE_TTL_SEC):
        # Another worker already holds a lease for this job_id.
        # Treat current payload as duplicate and skip it to avoid requeue storms.
        latest = get_report(report_id, user_id) or {}
        latest_status = str(latest.get("status") or "")
        if latest_status == JobStatus.queued.value:
            logger.debug("Skip duplicate queued payload with active running lease: report_id=%s job_id=%s", report_id, job_id)
        else:
            logger.debug("Skip payload with active running lease: report_id=%s job_id=%s", report_id, job_id)
        return

    if current_status == JobStatus.queued.value:
        update_report_status(report_id=report_id, status=JobStatus.running.value)

    output_schema = payload.get("output_schema")
    expected_json_template = payload.get("expected_json_template")
    analysis_columns = payload.get("analysis_columns") or []
    if not isinstance(analysis_columns, list):
        analysis_columns = []
    non_analysis_columns = payload.get("non_analysis_columns") or []
    if not isinstance(non_analysis_columns, list):
        non_analysis_columns = []

    file_path = UPLOADS_DIR / f"{payload['file_id']}.xlsx"
    if not file_path.exists():
        file_path = Path(file_path)

    api_key: str | None = None
    if str(payload.get("provider") or "") == "openai":
        encrypted = payload.get("api_key_encrypted")
        if encrypted:
            try:
                api_key = decrypt_text(str(encrypted))
            except Exception as exc:
                update_report_status(
                    report_id=report_id,
                    status=JobStatus.failed.value,
                    error_text="Не удалось расшифровать API-токен для worker-задачи",
                )
                logger.warning("Failed to decrypt queued API key for report %s: %s", report_id, exc)
                return
        if not api_key:
            api_key = os.getenv("OPENAI_API_KEY", "").strip() or None

    try:
        job = job_manager.create_job(
            job_id=job_id,
            report_id=report_id,
            file_path=file_path,
            user_id=user_id,
            provider=str(payload["provider"]),
            model=str(payload["model"]),
            prompt_template=str(payload["prompt_template"]),
            sheet_name=str(payload["sheet_name"]),
            analysis_columns=[str(item) for item in analysis_columns],
            non_analysis_columns=[str(item) for item in non_analysis_columns],
            group_by_column=str(payload.get("group_by_column") or "").strip() or None,
            group_max_rows=GROUP_MAX_ROWS,
            analysis_mode=str(payload.get("analysis_mode") or "custom"),
            output_schema=output_schema if isinstance(output_schema, dict) else None,
            expected_json_template=expected_json_template if isinstance(expected_json_template, dict) else None,
            max_reviews=int(payload.get("max_reviews") or 100),
            parallelism=int(payload.get("parallelism") or 3),
            temperature=float(payload.get("temperature") or 0),
            include_raw_json=bool(payload.get("include_raw_json", True)),
            use_cache=bool(payload.get("use_cache", True)),
            api_key=api_key,
        )

        last_heartbeat = 0.0
        while job.status in {JobStatus.queued, JobStatus.running, JobStatus.paused}:
            report_state = await asyncio.to_thread(get_report, report_id, user_id)
            desired = str((report_state or {}).get("status") or "")
            if desired in {JobStatus.failed.value, JobStatus.completed.value, JobStatus.canceled.value}:
                if job.status not in {JobStatus.completed, JobStatus.failed, JobStatus.canceled}:
                    logger.info("Worker cancel sync: report_id=%s job_id=%s desired=%s", report_id, job.id, desired)
                    await job_manager.cancel(job.id)
                # Не ждём завершения фоновой _run_job — она могла залипнуть на
                # долгом LLM-запросе и не дойти до проверки cancel_event. Освобождаем
                # воркер сразу, иначе новые задачи из очереди не подхватятся.
                # Если фон реально не завершится — recovery-pass переразметит отчёт.
                break
            elif desired == JobStatus.paused.value and job.status == JobStatus.running:
                logger.info("Worker pause sync: report_id=%s job_id=%s", report_id, job.id)
                await job_manager.pause(job.id)
            elif desired == JobStatus.running.value and job.status == JobStatus.paused:
                logger.info("Worker resume sync: report_id=%s job_id=%s", report_id, job.id)
                await job_manager.resume(job.id)
            elif desired == JobStatus.paused.value and job.status == JobStatus.paused:
                # Do not block the whole worker loop on a paused job.
                # Keep DB status "paused"; resume will enqueue and continue later.
                logger.info("Worker paused job released: report_id=%s job_id=%s", report_id, job.id)
                break
            elif desired == JobStatus.queued.value and job.status in {JobStatus.running, JobStatus.paused}:
                update_report_status(report_id=report_id, status=JobStatus.running.value)

            now = asyncio.get_running_loop().time()
            if now - last_heartbeat >= HEARTBEAT_INTERVAL_SEC:
                touch_running_lease(job_id, RUNNING_LEASE_TTL_SEC)
                await asyncio.to_thread(_heartbeat, job)
                last_heartbeat = now
            await asyncio.sleep(1.0)
    finally:
        release_running_lease(job_id)


async def _run_file_inspect_payload(payload: dict) -> None:
    file_id = str(payload.get("file_id") or "").strip()
    user_id = int(payload.get("user_id") or 0)
    if not file_id or user_id <= 0:
        return
    row = get_uploaded_file(file_id, user_id)
    if not row:
        return
    file_path = Path(str(row.get("path") or "")).resolve()
    if not file_path.exists():
        update_uploaded_file_inspect(
            file_id,
            user_id,
            inspect_status="error",
            inspect_error_text="Файл не найден на сервере",
        )
        return

    update_uploaded_file_inspect(
        file_id,
        user_id,
        inspect_status="parsing",
        inspect_error_text=None,
    )
    try:
        sheets_payload = await asyncio.to_thread(inspect_xlsx, file_path)
        suggested_sheet = sheets_payload[0]["name"] if sheets_payload else None
        suggested_column = None
        if sheets_payload and sheets_payload[0]["columns"]:
            candidates = sheets_payload[0]["columns"]
            preferred = [c for c in candidates if "review" in c.lower() or "отзыв" in c.lower()]
            suggested_column = preferred[0] if preferred else candidates[0]

        update_uploaded_file_inspect(
            file_id,
            user_id,
            inspect_status="ready",
            sheets=sheets_payload,
            suggested_sheet=suggested_sheet,
            suggested_column=suggested_column,
            inspect_error_text=None,
        )
        logger.info("File inspect worker success: file_id=%s user_id=%s sheets=%s", file_id, user_id, len(sheets_payload))
    except Exception as exc:
        update_uploaded_file_inspect(
            file_id,
            user_id,
            inspect_status="error",
            inspect_error_text=f"Не удалось прочитать xlsx: {exc}",
        )
        logger.warning("File inspect worker failed: file_id=%s user_id=%s error=%s", file_id, user_id, type(exc).__name__)


async def main() -> None:
    logger.info("Worker started: mode=%s", WORKER_MODE)

    run_analysis = WORKER_MODE in {"analysis", "all", ""}
    run_inspect = WORKER_MODE in {"inspect", "all"}

    if not run_analysis and not run_inspect:
        logger.error("Unsupported WORKER_MODE=%s (expected analysis|inspect|all)", WORKER_MODE)
        return

    if run_analysis:
        requeued, failed = await asyncio.to_thread(_recover_orphaned_reports_once)
        logger.info("Worker recovery pass: requeued=%s failed=%s", requeued, failed)

    async def recovery_loop() -> None:
        while True:
            try:
                r, f = await asyncio.to_thread(_recover_orphaned_reports_once)
                if r or f:
                    logger.info("Worker recovery scan: requeued=%s failed=%s", r, f)
            except Exception as exc:
                logger.exception("Worker recovery scan failed: %s", exc)
            await asyncio.sleep(RECOVERY_INTERVAL_SEC)

    recovery_task = asyncio.create_task(recovery_loop()) if run_analysis else None
    cleanup_task = None
    if run_analysis and REPORT_CLEANUP_ENABLED:
        async def cleanup_loop() -> None:
            while True:
                try:
                    deleted_reports, skipped_active, removed_uploads = await asyncio.to_thread(_run_report_cleanup_once)
                    if deleted_reports or skipped_active or removed_uploads:
                        logger.info(
                            "Report cleanup pass: deleted_reports=%s skipped_active=%s removed_orphan_uploads=%s keep_last=%s ttl_hours=%s",
                            deleted_reports,
                            skipped_active,
                            removed_uploads,
                            REPORT_KEEP_LAST,
                            UPLOAD_ORPHAN_TTL_HOURS,
                        )
                except Exception as exc:
                    logger.exception("Report cleanup scan failed: %s", exc)
                await asyncio.sleep(REPORT_CLEANUP_INTERVAL_SEC)

        cleanup_task = asyncio.create_task(cleanup_loop())
    while True:
        if run_analysis and run_inspect:
            payload = await asyncio.to_thread(dequeue_job, 1)
            if not payload:
                payload = await asyncio.to_thread(dequeue_inspect_job, 4)
        elif run_analysis:
            payload = await asyncio.to_thread(dequeue_job, 5)
        else:
            payload = await asyncio.to_thread(dequeue_inspect_job, 5)
        if not payload:
            await asyncio.sleep(0.2)
            continue
        try:
            await _run_payload(payload)
        except _PgOperationalError as exc:
            # BUG-15: разрыв соединения с Postgres (AdminShutdown при рестарте,
            # network glitch и т.п.). Пул с битыми коннектами непригоден —
            # сбрасываем, чтобы следующий _get_pg_pool() создал свежий.
            # Задачу возвращаем в очередь — без этого она навсегда потеряна
            # (dequeue уже удалил её из Redis-списка).
            logger.warning(
                "DB connection lost during worker task, will recover: %s",
                exc,
            )
            try:
                reset_pg_pool()
            except Exception as reset_exc:  # pragma: no cover - best effort
                logger.exception("Failed to reset PG pool after OperationalError: %s", reset_exc)
            kind = "file_inspect" if str(payload.get("kind") or "") == "file_inspect" else "analysis"
            if not requeue_after_transient_error(payload, kind=kind):
                logger.error(
                    "Failed to requeue payload after DB error; task may be lost: kind=%s payload_keys=%s",
                    kind,
                    sorted(payload.keys()),
                )
            # Короткая пауза даёт БД прийти в себя прежде чем воркер прочитает
            # задачу (возможно ту же самую) снова.
            await asyncio.sleep(1.0)
        except Exception as exc:
            logger.exception("Worker task failed: %s", exc)
            report_id = str(payload.get("report_id") or "")
            if report_id:
                try:
                    update_report_status(report_id=report_id, status=JobStatus.failed.value, error_text=str(exc))
                except Exception as status_exc:  # pragma: no cover
                    logger.exception("Failed to mark report failed after task error: %s", status_exc)
    if recovery_task:
        recovery_task.cancel()
    if cleanup_task:
        cleanup_task.cancel()


if __name__ == "__main__":
    asyncio.run(main())
