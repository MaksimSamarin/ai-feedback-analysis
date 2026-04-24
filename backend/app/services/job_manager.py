from __future__ import annotations

import asyncio
import functools
import hashlib
import json
import logging
import os
import time
import uuid
from dataclasses import dataclass, field
from datetime import date, datetime, timezone
from pathlib import Path
from typing import Any

import httpx
from pydantic import ValidationError

from app import __version__ as APP_VERSION
from app.config import (
    GLOBAL_LLM_PARALLELISM,
    LLM_RETRIES,
    REQUEST_TIMEOUT_SEC,
    RESULTS_DIR,
)
from app.db import (
    count_report_rows,
    find_semantic_cached_analysis,
    get_cached_analysis,
    get_redis_client,
    get_report_summary_agg,
    iter_report_rows,
    list_pending_group_keys_batch,
    list_pending_rows_by_group_key,
    list_pending_report_rows_batch,
    delete_cached_analysis,
    put_cached_analysis,
    put_semantic_cached_analysis,
    bulk_update_report_rows_same_result,
    update_report_progress,
    update_report_row_result,
    update_report_status,
    upsert_report_row_placeholders,
)
from app.embeddings import build_embedding
from app.config import (
    EMBEDDING_PROVIDER,
    SEMANTIC_CACHE_CANDIDATES,
    SEMANTIC_CACHE_ENABLED,
    SEMANTIC_CACHE_THRESHOLD,
)
from app.logging_utils import reset_request_id, set_request_id
from app.providers.base import ContextLengthExceeded
from app.providers.registry import build_provider
from app.schemas import AnalysisOutput, JobResult, JobStatus, JobSummary
from app.services.excel_service import export_raw_json, export_results_xlsx, iter_sheet_rows

logger = logging.getLogger("review_analyzer.job_manager")
JOB_PROGRESS_EMIT_INTERVAL_SEC = max(0.2, float(os.getenv("JOB_PROGRESS_EMIT_INTERVAL_SEC", "1.0")))
JOB_PROGRESS_EMIT_EVERY_ROWS = max(1, int(os.getenv("JOB_PROGRESS_EMIT_EVERY_ROWS", "20")))
JOB_PROGRESS_PERSIST_INTERVAL_SEC = max(0.2, float(os.getenv("JOB_PROGRESS_PERSIST_INTERVAL_SEC", "1.0")))
JOB_PROGRESS_PERSIST_EVERY_ROWS = max(1, int(os.getenv("JOB_PROGRESS_PERSIST_EVERY_ROWS", "20")))
# После завершения отчёта Job удаляется из JobManager.jobs с задержкой (BUG-01).
# Задержка нужна, чтобы поздние SSE-подписчики успели получить финальное событие.
JOB_CLEANUP_DELAY_SEC = max(0.0, float(os.getenv("JOB_CLEANUP_DELAY_SEC", "300")))


def _running_event() -> asyncio.Event:
    ev = asyncio.Event()
    ev.set()
    return ev


@dataclass
class Job:
    id: str
    file_path: Path
    user_id: int
    report_id: str
    provider: str
    model: str
    prompt_template: str
    sheet_name: str
    analysis_columns: list[str]
    non_analysis_columns: list[str]
    group_by_column: str | None
    group_max_rows: int
    analysis_mode: str
    output_schema: dict[str, Any] | None
    expected_json_template: dict[str, Any] | None
    max_reviews: int
    parallelism: int
    temperature: float
    include_raw_json: bool
    use_cache: bool
    created_at: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    started_at: datetime | None = None
    finished_at: datetime | None = None
    status: JobStatus = JobStatus.queued
    total: int = 0
    processed: int = 0
    current_step: str = "В очереди"
    logs: list[str] = field(default_factory=list)
    cache_hits: int = 0
    cache_misses: int = 0
    eta_seconds: float | None = None
    result: JobResult = field(default_factory=JobResult)
    cancel_event: asyncio.Event = field(default_factory=asyncio.Event)
    run_event: asyncio.Event = field(default_factory=_running_event)
    event_queue: asyncio.Queue[dict[str, Any]] = field(default_factory=asyncio.Queue)

    def add_log(self, message: str) -> None:
        ts = datetime.now().strftime("%H:%M:%S")
        self.logs.append(f"[{ts}] {message}")
        self.logs = self.logs[-200:]

    async def emit(self, event_type: str, payload: dict[str, Any]) -> None:
        event = {"type": event_type, "payload": payload}
        await self.event_queue.put(event)
        client = get_redis_client()
        if client is None:
            return
        channel = f"job_events:{self.id}"
        message = json.dumps(event, ensure_ascii=False)
        # Публикация — синхронная операция redis-py; выносим в поток,
        # чтобы не блокировать event loop (особенно при высокой частоте progress-событий).
        try:
            await asyncio.to_thread(client.publish, channel, message)
        except Exception:
            pass


class JobManager:
    def __init__(self) -> None:
        self.jobs: dict[str, Job] = {}
        self._llm_sem = asyncio.Semaphore(max(1, GLOBAL_LLM_PARALLELISM))
        self._semantic_cache_runtime_enabled = SEMANTIC_CACHE_ENABLED
        self._semantic_cache_disable_lock = asyncio.Lock()

    @staticmethod
    def _is_embedding_endpoint_404(exc: Exception) -> bool:
        if not isinstance(exc, httpx.HTTPStatusError):
            return False
        if exc.response is None or exc.response.status_code != 404:
            return False
        path = (exc.request.url.path if exc.request and exc.request.url else "").lower()
        return "/api/embed" in path or "/api/embeddings" in path

    async def _build_embedding_safe(
        self,
        normalized: str,
        api_key: str | None,
        warnings: list[str],
    ) -> list[float] | None:
        if not self._semantic_cache_runtime_enabled:
            return None
        embedding_api_key = api_key if EMBEDDING_PROVIDER == "openai" else None
        try:
            return await build_embedding(normalized, api_key=embedding_api_key)
        except Exception as exc:
            if self._is_embedding_endpoint_404(exc):
                async with self._semantic_cache_disable_lock:
                    if self._semantic_cache_runtime_enabled:
                        self._semantic_cache_runtime_enabled = False
                        warnings.append("semantic_cache_disabled_endpoint_404")
                        logger.warning(
                            "Semantic cache disabled at runtime: embedding endpoint returned 404 (provider=%s)",
                            EMBEDDING_PROVIDER,
                        )
                return None
            warnings.append("semantic_cache_error")
            return None

    def get(self, job_id: str) -> Job:
        job = self.jobs.get(job_id)
        if not job:
            raise KeyError("Job not found")
        return job

    def create_job(
        self,
        *,
        job_id: str | None = None,
        file_path: Path,
        user_id: int,
        report_id: str,
        provider: str,
        model: str,
        prompt_template: str,
        sheet_name: str,
        analysis_columns: list[str],
        non_analysis_columns: list[str] | None,
        group_by_column: str | None,
        group_max_rows: int,
        analysis_mode: str,
        output_schema: dict[str, Any] | None,
        expected_json_template: dict[str, Any] | None,
        max_reviews: int,
        parallelism: int,
        temperature: float,
        include_raw_json: bool,
        use_cache: bool,
        api_key: str | None,
        start_paused: bool = False,
    ) -> Job:
        job_id = job_id or str(uuid.uuid4())
        job = Job(
            id=job_id,
            file_path=file_path,
            user_id=user_id,
            report_id=report_id,
            provider=provider,
            model=model,
            prompt_template=prompt_template,
            sheet_name=sheet_name,
            analysis_columns=analysis_columns[:],
            non_analysis_columns=non_analysis_columns[:] if non_analysis_columns else [],
            group_by_column=(group_by_column or "").strip() or None,
            group_max_rows=max(1, int(group_max_rows or 100)),
            analysis_mode=analysis_mode,
            output_schema=output_schema,
            expected_json_template=expected_json_template,
            max_reviews=max_reviews,
            parallelism=parallelism,
            temperature=temperature,
            include_raw_json=include_raw_json,
            use_cache=use_cache,
        )
        if start_paused:
            job.status = JobStatus.paused
            job.current_step = "На паузе"
            job.run_event.clear()
        self.jobs[job_id] = job
        asyncio.create_task(self._run_job_with_cleanup(job, api_key=api_key))
        return job

    async def _run_job_with_cleanup(self, job: Job, *, api_key: str | None) -> None:
        """Запускает _run_job и планирует отложенное удаление из self.jobs.

        Словарь self.jobs нужен только для активных задач (cancel/pause/resume/SSE).
        После завершения держать Job в памяти смысла нет — данные отчёта уже в БД.
        Задержка нужна, чтобы поздний SSE-подписчик успел получить событие 'done'.
        См. BUG-01.
        """
        try:
            await self._run_job(job, api_key=api_key)
        finally:
            asyncio.create_task(self._delayed_cleanup(job.id))

    async def _delayed_cleanup(self, job_id: str, delay: float = JOB_CLEANUP_DELAY_SEC) -> None:
        try:
            await asyncio.sleep(delay)
        except asyncio.CancelledError:
            # При shutdown таск могут отменить — всё равно попробуем освободить память.
            pass
        self.jobs.pop(job_id, None)

    async def cancel(self, job_id: str) -> None:
        job = self.get(job_id)
        job.cancel_event.set()
        job.run_event.set()
        job.add_log("Запрошена отмена")
        await job.emit("status", {"status": "cancel_requested"})

    async def pause(self, job_id: str) -> None:
        job = self.get(job_id)
        if job.status != JobStatus.running:
            return
        job.status = JobStatus.paused
        job.run_event.clear()
        job.current_step = f"На паузе {job.processed}/{job.total}"
        job.add_log("Поставлено на паузу")
        payload = self._progress_payload(job)
        update_report_progress(
            report_id=job.report_id,
            total_rows=job.total,
            processed_rows=job.processed,
            progress_percent=float(payload["progress_percent"]),
            eta_seconds=job.eta_seconds,
            current_step=job.current_step,
        )
        update_report_status(report_id=job.report_id, status=JobStatus.paused.value)
        await job.emit("status", {"status": JobStatus.paused.value, "current_step": job.current_step, "logs": job.logs[-20:]})

    async def resume(self, job_id: str) -> None:
        job = self.get(job_id)
        if job.status != JobStatus.paused:
            return
        job.status = JobStatus.running
        job.current_step = f"Обработка {job.processed}/{job.total}"
        job.run_event.set()
        job.add_log("Снято с паузы")
        payload = self._progress_payload(job)
        update_report_progress(
            report_id=job.report_id,
            total_rows=job.total,
            processed_rows=job.processed,
            progress_percent=float(payload["progress_percent"]),
            eta_seconds=job.eta_seconds,
            current_step=job.current_step,
        )
        update_report_status(report_id=job.report_id, status=JobStatus.running.value)
        await job.emit("status", {"status": JobStatus.running.value, "current_step": job.current_step, "logs": job.logs[-20:]})

    async def _run_job(self, job: Job, api_key: str | None) -> None:
        logger.info(
            "Job run started: job_id=%s report_id=%s user_id=%s provider=%s model=%s",
            job.id,
            job.report_id,
            job.user_id,
            job.provider,
            job.model,
        )
        job.started_at = datetime.now(timezone.utc)
        if job.status == JobStatus.paused:
            job.add_log("Задача восстановлена в состоянии паузы")
            await job.emit("status", {"status": JobStatus.paused.value, "current_step": job.current_step, "logs": job.logs[-20:]})
            await job.run_event.wait()
            if job.cancel_event.is_set():
                return
        job.status = JobStatus.running
        job.current_step = "Подготовка данных"
        job.add_log("Подготовка данных")

        # Сначала проверяем существующие строки, чтобы не сбросить прогресс при resume/recovery.
        total_existing, processed_existing = await asyncio.to_thread(count_report_rows, job.report_id)
        if total_existing == 0:
            # Первый запуск — обнуляем прогресс явно.
            await asyncio.to_thread(
                update_report_progress,
                report_id=job.report_id,
                total_rows=0,
                processed_rows=0,
                progress_percent=0.0,
                eta_seconds=None,
                current_step=job.current_step,
            )
        else:
            # Resume/recovery — сохраняем прогресс, обновляем только шаг.
            resume_percent = (
                (float(processed_existing) * 100.0 / float(total_existing))
                if total_existing > 0
                else 0.0
            )
            await asyncio.to_thread(
                update_report_progress,
                report_id=job.report_id,
                total_rows=total_existing,
                processed_rows=processed_existing,
                progress_percent=resume_percent,
                eta_seconds=None,
                current_step=job.current_step,
            )
        await job.emit("status", {"message": "Подготовка данных"})

        try:
            # Переиспользуем результаты count_report_rows, сделанные выше.
            if total_existing == 0:
                job.add_log("Чтение Excel-файла")
                init_task = asyncio.create_task(
                    asyncio.to_thread(
                        self._initialize_report_rows_streaming,
                        report_id=job.report_id,
                        file_path=job.file_path,
                        sheet_name=job.sheet_name,
                        analysis_columns=job.analysis_columns,
                        non_analysis_columns=job.non_analysis_columns,
                        group_by_column=job.group_by_column,
                        max_reviews=job.max_reviews,
                        batch_size=2000,
                    )
                )
                init_started = time.perf_counter()
                heartbeat_idx = 0
                while not init_task.done():
                    elapsed = int(time.perf_counter() - init_started)
                    job.current_step = f"Подготовка данных: чтение Excel ({elapsed} с)"
                    if heartbeat_idx % 5 == 0:
                        job.add_log(f"Подготовка данных... {elapsed} с")
                    heartbeat_idx += 1
                    await asyncio.to_thread(
                        update_report_progress,
                        report_id=job.report_id,
                        total_rows=0,
                        processed_rows=0,
                        progress_percent=0.0,
                        eta_seconds=None,
                        current_step=job.current_step,
                    )
                    await job.emit(
                        "status",
                        {
                            "status": job.status.value,
                            "current_step": job.current_step,
                            "logs": job.logs[-20:],
                        },
                    )
                    await asyncio.sleep(2)
                inserted = await init_task
                job.add_log(f"Инициализировано строк: {inserted}")
                total_existing, processed_existing = await asyncio.to_thread(count_report_rows, job.report_id)

            job.total = total_existing
            job.processed = processed_existing

            provider = build_provider(job.provider)
            pending_total = max(0, job.total - job.processed)
            job.add_log(f"Всего строк: {job.total}, осталось: {pending_total}")
            job.add_log(f"Параллелизм: {job.parallelism}")
            job.add_log(f"Temperature: {job.temperature}")
            job.add_log(f"Кэш LLM: {'включен' if job.use_cache else 'выключен'}")

            progress_payload = self._progress_payload(job)
            await asyncio.to_thread(
                update_report_progress,
                report_id=job.report_id,
                total_rows=job.total,
                processed_rows=job.processed,
                progress_percent=float(progress_payload["progress_percent"]),
                eta_seconds=job.eta_seconds,
                current_step="Обработка",
            )
            await job.emit("progress", progress_payload)

            started = time.perf_counter()
            progress_lock = asyncio.Lock()
            last_progress_emit_ts = started
            last_progress_emit_processed = job.processed
            last_progress_persist_ts = started
            last_progress_persist_processed = job.processed
            queue: asyncio.Queue[dict[str, Any]] = asyncio.Queue(maxsize=max(200, job.parallelism * 200))
            producer_done = asyncio.Event()

            async def producer() -> None:
                try:
                    if not job.group_by_column:
                        after_row_number = 0
                        batch_limit = 2000
                        while not job.cancel_event.is_set():
                            batch_rows = await asyncio.to_thread(
                                list_pending_report_rows_batch,
                                job.report_id,
                                after_row_number=after_row_number,
                                limit=batch_limit,
                            )
                            if not batch_rows:
                                break
                            for row in batch_rows:
                                warnings = []
                                raw_warnings = row.get("warnings_json")
                                if raw_warnings:
                                    try:
                                        warnings = json.loads(raw_warnings)
                                    except Exception:
                                        warnings = []
                                await queue.put(
                                    {
                                        "kind": "row",
                                        "rows_count": 1,
                                        "row": {
                                            "row_number": int(row["row_number"]),
                                            "review_text": row.get("review_text"),
                                            "input_json": row.get("input_json"),
                                            "warnings": warnings,
                                        },
                                    }
                                )
                            after_row_number = int(batch_rows[-1]["row_number"])
                        return

                    group_batch_limit = 400
                    group_offset = 0
                    while not job.cancel_event.is_set():
                        groups = await asyncio.to_thread(
                            list_pending_group_keys_batch,
                            job.report_id,
                            limit=group_batch_limit,
                            offset=group_offset,
                        )
                        if not groups:
                            break
                        group_offset += len(groups)
                        for group_meta in groups:
                            group_key = str(group_meta.get("group_key") or "")
                            group_rows_raw = await asyncio.to_thread(
                                list_pending_rows_by_group_key,
                                job.report_id,
                                group_key,
                            )
                            rows_for_group: list[dict[str, Any]] = []
                            for row in group_rows_raw:
                                warnings = []
                                raw_warnings = row.get("warnings_json")
                                if raw_warnings:
                                    try:
                                        warnings = json.loads(raw_warnings)
                                    except Exception:
                                        warnings = []
                                rows_for_group.append(
                                    {
                                        "row_number": int(row["row_number"]),
                                        "review_text": row.get("review_text"),
                                        "input_json": row.get("input_json"),
                                        "warnings": warnings,
                                    }
                                )
                            group_size = len(rows_for_group)
                            if group_size <= 0:
                                continue
                            if group_size > job.group_max_rows:
                                await queue.put(
                                    {
                                        "kind": "skip_group",
                                        "rows_count": group_size,
                                        "rows": rows_for_group,
                                        "warning": f"skipped_large_group:{group_size}>{job.group_max_rows}",
                                        "error_text": (
                                            f"Пропущено: группа слишком большая ({group_size} строк > лимита {job.group_max_rows})."
                                        ),
                                        "row": {
                                            "row_number": int(rows_for_group[0]["row_number"]),
                                            "review_text": str(rows_for_group[0].get("review_text") or ""),
                                            "input_json": rows_for_group[0].get("input_json"),
                                            "warnings": list(rows_for_group[0].get("warnings") or []),
                                        },
                                    }
                                )
                                continue
                            combined_review = "\n".join(
                                f"[row {int(item['row_number'])}] {str(item.get('review_text') or '')}"
                                for item in rows_for_group
                            )
                            group_payload = {
                                "group_key": group_key,
                                "group_size": group_size,
                                "rows": [
                                    {
                                        "row_number": int(item["row_number"]),
                                        "review_text": str(item.get("review_text") or ""),
                                        "row_json": self._safe_json_dict(item.get("input_json")),
                                    }
                                    for item in rows_for_group
                                ],
                            }
                            merged_row = {
                                "row_number": int(rows_for_group[0]["row_number"]),
                                "review_text": combined_review,
                                "input_json": json.dumps(group_payload, ensure_ascii=False),
                                "warnings": [f"grouped_by:{job.group_by_column}", f"group_size:{group_size}"],
                            }
                            await queue.put(
                                {
                                    "kind": "group",
                                    "rows_count": group_size,
                                    "rows": rows_for_group,
                                    "row": merged_row,
                                }
                            )
                finally:
                    producer_done.set()

            async def worker() -> None:
                nonlocal last_progress_emit_ts
                nonlocal last_progress_emit_processed
                nonlocal last_progress_persist_ts
                nonlocal last_progress_persist_processed
                while not job.cancel_event.is_set():
                    await job.run_event.wait()
                    if job.cancel_event.is_set():
                        break
                    try:
                        work_item = queue.get_nowait()
                    except asyncio.QueueEmpty:
                        if producer_done.is_set():
                            break
                        await asyncio.sleep(0.05)
                        continue

                    try:
                        row = dict(work_item.get("row") or {})
                        mode = str(work_item.get("kind") or "row")
                        grouped_rows = work_item.get("rows") if mode == "group" else None
                        if mode == "skip_group":
                            skipped_rows = work_item.get("rows") if isinstance(work_item.get("rows"), list) else []
                            skip_error_text = str(work_item.get("error_text") or "Пропущено")
                            skipped_numbers = [int(r["row_number"]) for r in skipped_rows]
                            # Один batch-UPDATE вместо N отдельных (v2.0.0, итерация 3.2).
                            await asyncio.to_thread(
                                bulk_update_report_rows_same_result,
                                report_id=job.report_id,
                                row_numbers=skipped_numbers,
                                sentiment_label=None,
                                negativity_score=None,
                                short_reason=None,
                                category=None,
                                raw_response=None,
                                error_text=skip_error_text,
                                custom_data=None,
                            )
                            continue
                        async with self._llm_sem:
                            req_token = set_request_id(
                                f"u:{job.user_id}|r:{job.report_id}|j:{job.id}|row:{int(row.get('row_number') or 0)}"
                            )
                            try:
                                result_row = await self._process_row(
                                    provider=provider,
                                    row=row,
                                    prompt_template=job.prompt_template,
                                    model=job.model,
                                    api_key=api_key,
                                    analysis_mode=job.analysis_mode,
                                    output_schema=job.output_schema,
                                    expected_json_template=job.expected_json_template,
                                    use_cache=job.use_cache,
                                    temperature=job.temperature,
                                )
                            finally:
                                reset_request_id(req_token)
                        warnings = result_row.get("warnings") or []
                        if "cache_disabled" in warnings:
                            pass
                        elif "cache_hit" in warnings:
                            job.cache_hits += 1
                        else:
                            job.cache_misses += 1
                        category_value = result_row.get("category")
                        stored_category = category_value.strip() if isinstance(category_value, str) and category_value.strip() else None
                        target_rows: list[dict[str, Any]] = list(grouped_rows) if isinstance(grouped_rows, list) else [row]
                        prompt_tokens_total = int(result_row.get("_usage_prompt_tokens") or 0)
                        completion_tokens_total = int(result_row.get("_usage_completion_tokens") or 0)
                        total_tokens_total = int(result_row.get("_usage_total_tokens") or 0)
                        target_numbers = [int(t["row_number"]) for t in target_rows]
                        # Один batch-UPDATE для всей группы (v2.0.0, итерация 3.2).
                        # Раньше N отдельных UPDATE зажимали PG на группах 20k строк —
                        # теперь 1-4 UPDATE батчами по 5k. Токены на первой строке, чтобы
                        # при SUM(total_tokens) отчёта получить точную сумму вызовов LLM.
                        await asyncio.to_thread(
                            bulk_update_report_rows_same_result,
                            report_id=job.report_id,
                            row_numbers=target_numbers,
                            sentiment_label=result_row.get("sentiment_label"),
                            negativity_score=result_row.get("negativity_score"),
                            short_reason=result_row.get("summary"),
                            category=stored_category,
                            raw_response=result_row.get("raw_response"),
                            error_text=result_row.get("error"),
                            custom_data=result_row.get("analysis_json"),
                            total_prompt_tokens=prompt_tokens_total,
                            total_completion_tokens=completion_tokens_total,
                            total_total_tokens=total_tokens_total,
                        )
                    finally:
                        queue.task_done()

                    async with progress_lock:
                        job.processed += max(1, int(work_item.get("rows_count") or 1))
                        elapsed = time.perf_counter() - started
                        avg = elapsed / max(1, job.processed - processed_existing)
                        remaining = max(0, job.total - job.processed)
                        job.eta_seconds = round(avg * remaining, 1) if remaining else 0.0
                        if job.status == JobStatus.running:
                            job.current_step = f"Обработка {job.processed}/{job.total}"

                        payload = self._progress_payload(job)
                        now_ts = time.perf_counter()
                        progress_done = job.processed >= job.total
                        progressed_since_emit = job.processed - last_progress_emit_processed
                        progressed_since_persist = job.processed - last_progress_persist_processed
                        should_emit = (
                            progress_done
                            or progressed_since_emit >= JOB_PROGRESS_EMIT_EVERY_ROWS
                            or (now_ts - last_progress_emit_ts) >= JOB_PROGRESS_EMIT_INTERVAL_SEC
                        )
                        should_persist = (
                            progress_done
                            or progressed_since_persist >= JOB_PROGRESS_PERSIST_EVERY_ROWS
                            or (now_ts - last_progress_persist_ts) >= JOB_PROGRESS_PERSIST_INTERVAL_SEC
                        )
                        if should_persist:
                            await asyncio.to_thread(
                                update_report_progress,
                                report_id=job.report_id,
                                total_rows=job.total,
                                processed_rows=job.processed,
                                progress_percent=float(payload["progress_percent"]),
                                eta_seconds=job.eta_seconds,
                                current_step=job.current_step,
                            )
                            last_progress_persist_ts = now_ts
                            last_progress_persist_processed = job.processed
                        if should_emit:
                            await job.emit("progress", payload)
                            last_progress_emit_ts = now_ts
                            last_progress_emit_processed = job.processed

            worker_count = max(1, min(job.parallelism, max(1, pending_total)))
            producer_task = asyncio.create_task(producer())
            workers = [asyncio.create_task(worker()) for _ in range(worker_count)]
            try:
                await asyncio.gather(*workers)
                await producer_task
            finally:
                if not producer_task.done():
                    producer_task.cancel()

            if job.cancel_event.is_set():
                job.status = JobStatus.canceled
                job.current_step = "Отменено пользователем"
                job.add_log("Обработка отменена пользователем")
                job.finished_at = datetime.now(timezone.utc)
                await asyncio.to_thread(
                    update_report_progress,
                    report_id=job.report_id,
                    total_rows=job.total,
                    processed_rows=job.processed,
                    progress_percent=float(self._progress_payload(job)["progress_percent"]),
                    eta_seconds=job.eta_seconds,
                    current_step=job.current_step,
                )
                await asyncio.to_thread(
                    update_report_status,
                    report_id=job.report_id,
                    status=JobStatus.canceled.value,
                    finished_at=job.finished_at.isoformat(),
                )
                logger.info("Job canceled: job_id=%s processed=%s total=%s", job.id, job.processed, job.total)
                await job.emit("done", {"status": job.status.value})
                return

            # Перед тяжёлой агрегацией и экспортом проверяем отмену, чтобы не уйти в
            # многоминутный to_thread который нельзя прервать (BUG-13).
            if await self._handle_cancel_before_finalize(job, step="Формирование итогов"):
                return

            job.current_step = "Формирование итогов"
            await asyncio.to_thread(
                update_report_progress,
                report_id=job.report_id,
                total_rows=job.total,
                processed_rows=job.processed,
                progress_percent=float(self._progress_payload(job)["progress_percent"]),
                eta_seconds=job.eta_seconds,
                current_step=job.current_step,
            )
            await job.emit("status", {"current_step": job.current_step})

            # Стриминговая финализация (BUG-14): читаем БД батчами, не грузим 700к строк в память.
            summary_payload = await asyncio.to_thread(
                self._build_summary_from_db, job.report_id
            )
            job.result.summary = JobSummary(**summary_payload)

            if await self._handle_cancel_before_finalize(job, step="Формирование итогов"):
                return

            RESULTS_DIR.mkdir(parents=True, exist_ok=True)
            result_xlsx = RESULTS_DIR / f"{job.id}_results.xlsx"

            # Первая строка нужна только для prompt_example — без загрузки всего отчёта.
            prompt_example: str | None = None
            first_row = await asyncio.to_thread(self._get_first_report_row, job.report_id)
            if first_row:
                prompt_example = self._render_prompt(
                    prompt_template=job.prompt_template,
                    review_text=str(first_row.get("review_text") or ""),
                    input_json=(
                        json.dumps(first_row.get("input_json"), ensure_ascii=False)
                        if isinstance(first_row.get("input_json"), dict)
                        else str(first_row.get("input_json") or "")
                    ),
                    analysis_mode=job.analysis_mode,
                    expected_json_template=job.expected_json_template,
                )

            if await self._handle_cancel_before_finalize(job, step="Сохранение выгрузки"):
                return

            job.current_step = "Сохранение выгрузки"
            await asyncio.to_thread(
                update_report_progress,
                report_id=job.report_id,
                total_rows=job.total,
                processed_rows=job.processed,
                progress_percent=float(self._progress_payload(job)["progress_percent"]),
                eta_seconds=job.eta_seconds,
                current_step=job.current_step,
            )
            await job.emit("status", {"current_step": job.current_step})

            report_id = job.report_id

            def _rows_factory():
                return iter_report_rows(report_id, batch_size=2000)

            await asyncio.to_thread(
                functools.partial(
                    export_results_xlsx,
                    result_xlsx,
                    _rows_factory,
                    summary_payload,
                    prompt_example=prompt_example,
                    group_by_column=job.group_by_column,
                )
            )
            job.result.results_file = result_xlsx.name

            if job.include_raw_json:
                raw_file = RESULTS_DIR / f"{job.id}_raw.json"
                await asyncio.to_thread(
                    export_raw_json,
                    raw_file,
                    rows_factory=_rows_factory,
                    model=job.model,
                    provider=job.provider,
                    prompt_template=job.prompt_template,
                    app_version=APP_VERSION,
                )
                job.result.raw_file = raw_file.name

            job.status = JobStatus.completed
            job.current_step = "Завершено"
            job.finished_at = datetime.now(timezone.utc)
            job.eta_seconds = 0.0
            job.add_log("Обработка завершена")
            job.add_log(f"Кэш LLM: hits={job.cache_hits}, misses={job.cache_misses}")
            await asyncio.to_thread(
                update_report_progress,
                report_id=job.report_id,
                total_rows=job.total,
                processed_rows=job.total,
                progress_percent=100.0,
                eta_seconds=0.0,
                current_step=job.current_step,
            )
            await asyncio.to_thread(
                update_report_status,
                report_id=job.report_id,
                status=JobStatus.completed.value,
                finished_at=job.finished_at.isoformat(),
                results_file=job.result.results_file,
                raw_file=job.result.raw_file,
                summary=summary_payload,
            )
            logger.info(
                "Job completed: job_id=%s processed=%s total=%s cache_hits=%s cache_misses=%s",
                job.id,
                job.processed,
                job.total,
                job.cache_hits,
                job.cache_misses,
            )
            await job.emit("done", {"status": job.status.value, "result": job.result.model_dump()})
        except Exception as exc:
            job.status = JobStatus.failed
            job.current_step = "Ошибка"
            job.finished_at = datetime.now(timezone.utc)
            job.add_log(f"Ошибка: {type(exc).__name__}: {exc}")
            await asyncio.to_thread(
                update_report_progress,
                report_id=job.report_id,
                total_rows=job.total,
                processed_rows=job.processed,
                progress_percent=float(self._progress_payload(job)["progress_percent"]),
                eta_seconds=job.eta_seconds,
                current_step=job.current_step,
            )
            await asyncio.to_thread(
                update_report_status,
                report_id=job.report_id,
                status=JobStatus.failed.value,
                finished_at=job.finished_at.isoformat(),
                error_text=f"{type(exc).__name__}: {exc}",
            )
            logger.exception("Job failed: job_id=%s error=%s", job.id, exc)
            await job.emit("error", {"message": str(exc), "type": type(exc).__name__})

    def _initialize_report_rows_streaming(
        self,
        *,
        report_id: str,
        file_path: Path,
        sheet_name: str,
        analysis_columns: list[str],
        non_analysis_columns: list[str],
        group_by_column: str | None,
        max_reviews: int,
        batch_size: int = 2000,
    ) -> int:
        batch: list[dict[str, Any]] = []
        inserted_total = 0
        chunk = max(1, batch_size)
        for row in iter_sheet_rows(
            file_path,
            sheet_name=sheet_name,
            analysis_columns=analysis_columns,
            non_analysis_columns=non_analysis_columns,
            max_reviews=max_reviews,
        ):
            group_key: str | None = None
            if group_by_column:
                try:
                    parsed_input = json.loads(str(row.get("input_json") or "{}"))
                except Exception:
                    parsed_input = {}
                group_raw = parsed_input.get(group_by_column) if isinstance(parsed_input, dict) else None
                group_key = str(group_raw or "").strip() or f"__missing__:{int(row.get('row_number') or 0)}"
                if str(group_raw or "").strip() == "":
                    warnings = list(row.get("warnings") or [])
                    if "group_key_missing" not in warnings:
                        warnings.append("group_key_missing")
                    row["warnings"] = warnings
            row["group_key"] = group_key
            batch.append(row)
            if len(batch) >= chunk:
                upsert_report_row_placeholders(report_id=report_id, rows=batch, batch_size=chunk)
                inserted_total += len(batch)
                batch.clear()

        if batch:
            upsert_report_row_placeholders(report_id=report_id, rows=batch, batch_size=chunk)
            inserted_total += len(batch)

        return inserted_total

    async def _process_row(
        self,
        *,
        provider,
        row: dict[str, Any],
        prompt_template: str,
        model: str,
        api_key: str | None,
        analysis_mode: str,
        output_schema: dict[str, Any] | None,
        expected_json_template: dict[str, Any] | None,
        use_cache: bool,
        temperature: float,
    ) -> dict[str, Any]:
        out = {
            "row_number": row["row_number"],
            "review_text": row["review_text"],
            "warnings": list(row.get("warnings") or []),
            "error": None,
            "category": None,
            "sentiment_label": None,
            "negativity_score": None,
            "summary": None,
            "analysis_json": None,
            "raw_response": None,
            "_usage_prompt_tokens": 0,
            "_usage_completion_tokens": 0,
            "_usage_total_tokens": 0,
        }
        input_json = str(row.get("input_json") or "")
        if not row.get("review_text"):
            if input_json:
                try:
                    parsed_input = json.loads(input_json)
                except Exception:
                    parsed_input = None
                if isinstance(parsed_input, dict) and not any(v not in (None, "") for v in parsed_input.values()):
                    out["warnings"].append("skipped_empty")
                    return out
            else:
                out["warnings"].append("skipped_empty")
                return out

        review_text = str(row.get("review_text") or "")
        normalized = " ".join((input_json or review_text).split()).strip()
        cache_key, prompt_hash, review_hash, expected_template_hash, output_schema_hash = (
            self._compute_cache_key(
                provider_id=provider.id,
                model=model,
                analysis_mode=analysis_mode,
                prompt_template=prompt_template,
                expected_json_template=expected_json_template,
                output_schema=output_schema,
                normalized_review=normalized,
            )
        )
        semantic_embedding: list[float] | None = None
        semantic_checked = False

        last_exc: Exception | None = None
        last_validation_error: str | None = None
        last_parsed: Any = None
        for attempt in range(LLM_RETRIES + 1):
            retry_feedback: dict[str, Any] | None = None
            if attempt > 0 and last_validation_error:
                retry_feedback = {
                    "error": last_validation_error,
                    "previous_response": last_parsed,
                }
            try:
                # При повторной попытке c retry_feedback кэш пропускаем — иначе модель
                # снова вернёт тот же битый ответ из кэша и обратная связь валидатора
                # не дойдёт до неё. На retry всегда свежий запрос.
                if use_cache and not retry_feedback:
                    cached = get_cached_analysis(cache_key)
                    if cached:
                        parsed = cached.get("analysis") or {}
                        raw = cached.get("raw")
                        try:
                            if analysis_mode == "sentiment":
                                validated = AnalysisOutput(**parsed)
                                payload = validated.model_dump()
                            else:
                                payload = self._validate_custom_output(
                                    parsed,
                                    output_schema,
                                    expected_json_template=expected_json_template,
                                    warnings=out["warnings"],
                                )
                                out["analysis_json"] = payload
                        except (ValidationError, ValueError) as ve:
                            # Кэш битый (например, схема поменялась, формат старый).
                            # Удаляем запись и идём в модель — не раньше, пока retry
                            # с таким же ключом снова даст тот же битый ответ.
                            try:
                                delete_cached_analysis(cache_key)
                            except Exception:
                                pass
                            out["warnings"].append(f"cache_invalidated:{ve!s:.80}")
                        else:
                            out.update({k: v for k, v in payload.items() if k in out})
                            out["raw_response"] = raw
                            out["warnings"].append("cache_hit")
                            usage = raw.get("usage") if isinstance(raw, dict) else None
                            if isinstance(usage, dict):
                                out["_usage_prompt_tokens"] = int(usage.get("prompt_tokens") or 0)
                                out["_usage_completion_tokens"] = int(usage.get("completion_tokens") or 0)
                                out["_usage_total_tokens"] = int(usage.get("total_tokens") or 0)
                            return out

                    if self._semantic_cache_runtime_enabled and not semantic_checked and normalized:
                        if semantic_embedding is None:
                            semantic_embedding = await self._build_embedding_safe(normalized, api_key, out["warnings"])
                        semantic_checked = True
                        if semantic_embedding is not None:
                            semantic = find_semantic_cached_analysis(
                                provider=provider.id,
                                model=model,
                                analysis_mode=analysis_mode,
                                prompt_hash=prompt_hash,
                                expected_template_hash=expected_template_hash,
                                output_schema_hash=output_schema_hash,
                                embedding=semantic_embedding,
                                threshold=SEMANTIC_CACHE_THRESHOLD,
                                candidates=SEMANTIC_CACHE_CANDIDATES,
                            )
                            if semantic:
                                parsed = semantic.get("analysis") or {}
                                raw = semantic.get("raw")
                                if analysis_mode == "sentiment":
                                    validated = AnalysisOutput(**parsed)
                                    payload = validated.model_dump()
                                else:
                                    payload = self._validate_custom_output(
                                        parsed,
                                        output_schema,
                                        expected_json_template=expected_json_template,
                                        warnings=out["warnings"],
                                    )
                                    out["analysis_json"] = payload
                                out.update({k: v for k, v in payload.items() if k in out})
                                out["raw_response"] = raw
                                out["warnings"].append(f"semantic_cache_hit:{semantic.get('similarity', 0)}")
                                usage = raw.get("usage") if isinstance(raw, dict) else None
                                if isinstance(usage, dict):
                                    out["_usage_prompt_tokens"] = int(usage.get("prompt_tokens") or 0)
                                    out["_usage_completion_tokens"] = int(usage.get("completion_tokens") or 0)
                                    out["_usage_total_tokens"] = int(usage.get("total_tokens") or 0)
                                return out
                else:
                    out["warnings"].append("cache_disabled")

                prompt = self._render_prompt(
                    prompt_template=prompt_template,
                    review_text=review_text,
                    input_json=input_json,
                    analysis_mode=analysis_mode,
                    expected_json_template=expected_json_template,
                    retry_feedback=retry_feedback,
                )
                parsed, raw = await provider.analyze(
                    prompt,
                    model,
                    api_key,
                    REQUEST_TIMEOUT_SEC,
                    temperature,
                )
                try:
                    if analysis_mode == "sentiment":
                        validated = AnalysisOutput(**parsed)
                        payload = validated.model_dump()
                    else:
                        payload = self._validate_custom_output(
                            parsed,
                            output_schema,
                            expected_json_template=expected_json_template,
                            warnings=out["warnings"],
                        )
                        out["analysis_json"] = payload
                except (ValidationError, ValueError) as ve:
                    # Сохраняем невалидный ответ и текст ошибки, чтобы на следующей попытке
                    # передать модели обратную связь через retry_feedback в промпте.
                    last_validation_error = str(ve)
                    last_parsed = parsed
                    raise
                out.update({k: v for k, v in payload.items() if k in out})
                out["raw_response"] = raw
                usage = raw.get("usage") if isinstance(raw, dict) else None
                if isinstance(usage, dict):
                    out["_usage_prompt_tokens"] = int(usage.get("prompt_tokens") or 0)
                    out["_usage_completion_tokens"] = int(usage.get("completion_tokens") or 0)
                    out["_usage_total_tokens"] = int(usage.get("total_tokens") or 0)
                else:
                    # Fallback estimate for providers that do not return usage.
                    est_prompt = max(1, len(str(row.get("review_text") or "")) // 4)
                    est_completion = 40
                    out["_usage_prompt_tokens"] = est_prompt
                    out["_usage_completion_tokens"] = est_completion
                    out["_usage_total_tokens"] = est_prompt + est_completion

                if use_cache:
                    put_cached_analysis(
                        cache_key=cache_key,
                        provider=provider.id,
                        model=model,
                        prompt_hash=prompt_hash,
                        review_hash=review_hash,
                        analysis=payload,
                        raw=raw if isinstance(raw, dict) else None,
                    )
                    if self._semantic_cache_runtime_enabled and normalized:
                        if semantic_embedding is None:
                            semantic_embedding = await self._build_embedding_safe(normalized, api_key, out["warnings"])
                        if semantic_embedding is not None:
                            try:
                                put_semantic_cached_analysis(
                                    provider=provider.id,
                                    model=model,
                                    analysis_mode=analysis_mode,
                                    prompt_hash=prompt_hash,
                                    expected_template_hash=expected_template_hash,
                                    output_schema_hash=output_schema_hash,
                                    embedding=semantic_embedding,
                                    analysis=payload,
                                    raw=raw if isinstance(raw, dict) else None,
                                )
                            except Exception:
                                out["warnings"].append("semantic_cache_store_error")
                return out
            except ContextLengthExceeded as exc:
                out["error"] = (
                    f"Промпт превысил контекст модели `{exc.model}`. "
                    "Уменьшите размер группы (`group_max_rows`) или используйте модель с большим контекстом."
                )
                if exc.provider_message:
                    out["warnings"].append(
                        f"context_length_exceeded:{exc.provider_message[:200]}"
                    )
                return out
            except (ValidationError, ValueError) as exc:
                # Валидационные ошибки — last_validation_error/last_parsed уже
                # установлены во внутреннем try, они попадут в retry_feedback.
                last_exc = exc
            except (json.JSONDecodeError, TimeoutError) as exc:
                # Сетевые/парсинговые ошибки — обратной связи для модели нет.
                last_exc = exc
                last_validation_error = None
                last_parsed = None
            except Exception as exc:
                last_exc = exc
                last_validation_error = None
                last_parsed = None

            await asyncio.sleep(0.5 * (attempt + 1))

        out["error"] = f"Ошибка LLM после повторов: {type(last_exc).__name__}: {last_exc}"
        return out

    @staticmethod
    def _safe_json_dict(raw: Any) -> dict[str, Any]:
        text = str(raw or "").strip()
        if not text:
            return {}
        try:
            parsed = json.loads(text)
            return parsed if isinstance(parsed, dict) else {}
        except Exception:
            return {}

    def _render_prompt(
        self,
        *,
        prompt_template: str,
        review_text: str,
        input_json: str,
        analysis_mode: str,
        expected_json_template: dict[str, Any] | None,
        retry_feedback: dict[str, Any] | None = None,
    ) -> str:
        prompt = prompt_template
        data_text = input_json or "{}"
        if "{row_json}" in prompt:
            prompt = prompt.replace("{row_json}", data_text)
        else:
            prompt = f"{prompt.rstrip()}\n\nДанные строки:\n{data_text}"
        if analysis_mode == "custom" and expected_json_template:
            expected_json_text = json.dumps(expected_json_template, ensure_ascii=False, indent=2)
            prompt = (
                f"{prompt.strip()}\n\n"
                "Верни только валидный JSON без markdown и без лишнего текста.\n"
                "Промпт выше определяет смысл заполнения полей.\n"
                "EXPECTED_JSON ниже определяет только структуру ответа, типы данных и допустимые значения.\n"
                "Используй те же ключи, что и в EXPECTED_JSON.\n"
                "Не добавляй новые поля и не возвращай входные поля строки, если их нет в EXPECTED_JSON.\n"
                "Не копируй служебные значения схемы (`type`, `values`, `min`, `max`, `items`, `properties`) в ответ.\n"
                "Если у поля `type: enum`, верни одно значение из его массива `values`.\n"
                "EXPECTED_JSON:\n"
                f"{expected_json_text}\n"
            )
        if retry_feedback:
            error_text = str(retry_feedback.get("error") or "").strip()
            previous_response = retry_feedback.get("previous_response")
            if previous_response is not None:
                try:
                    previous_text = json.dumps(previous_response, ensure_ascii=False)
                except Exception:
                    previous_text = str(previous_response)
            else:
                previous_text = ""
            if len(previous_text) > 500:
                previous_text = previous_text[:500] + "…"
            feedback_lines = [
                "",
                "Предыдущий ответ не прошёл проверку — исправь и верни заново.",
            ]
            if error_text:
                feedback_lines.append(f"Причина ошибки: {error_text}")
            if previous_text:
                feedback_lines.append(f"Прошлый ответ (сокращён до 500 символов): {previous_text}")
            feedback_lines.append("Верни корректный JSON строго по EXPECTED_JSON, без повторения ошибок.")
            prompt = f"{prompt.rstrip()}\n" + "\n".join(feedback_lines) + "\n"
        return prompt

    def _validate_custom_output(
        self,
        parsed: Any,
        output_schema: dict[str, Any] | None,
        *,
        expected_json_template: dict[str, Any] | None = None,
        warnings: list[str] | None = None,
    ) -> dict[str, Any]:
        if not isinstance(parsed, dict):
            raise ValueError("Ожидался JSON-объект в ответе модели")

        if expected_json_template:
            expected_keys = set(expected_json_template.keys())
            matched_keys = [key for key in parsed.keys() if key in expected_keys]
            if not matched_keys:
                raise ValueError("Модель вернула входные данные вместо результата по Ожидаемому JSON")

            extra_keys = [key for key in parsed.keys() if key not in expected_keys]
            if extra_keys and warnings is not None:
                warnings.append(f"dropped_extra_keys:{','.join(extra_keys[:5])}")
            parsed = {key: parsed[key] for key in parsed.keys() if key in expected_keys}

        if not output_schema:
            return parsed

        schema_type = output_schema.get("type")
        if schema_type and schema_type != "object":
            raise ValueError("Поддерживается output_schema только с type=object")

        required = output_schema.get("required") or []
        if isinstance(required, list):
            for field in required:
                if field not in parsed or parsed.get(field) is None:
                    raise ValueError(f"В ответе отсутствует обязательное поле: {field}")

        properties = output_schema.get("properties") or {}
        if isinstance(properties, dict):
            for field, field_schema in properties.items():
                if field not in parsed:
                    continue
                self._validate_field_type(field, parsed[field], field_schema if isinstance(field_schema, dict) else {})

        return parsed

    def _validate_field_type(self, field: str, value: Any, schema: dict[str, Any]) -> None:
        expected = schema.get("type")
        if not expected:
            return
        if expected == "string" and not isinstance(value, str):
            raise ValueError(f"Поле '{field}' должно быть строкой")
        min_length = schema.get("min_length")
        max_length = schema.get("max_length")
        if expected == "string" and isinstance(min_length, int) and len(value) < min_length:
            raise ValueError(f"Поле '{field}' должно содержать не менее {min_length} символов")
        if expected == "string" and isinstance(max_length, int) and len(value) > max_length:
            raise ValueError(f"Поле '{field}' должно содержать не более {max_length} символов")
        fmt = schema.get("format")
        if expected == "string" and fmt == "date":
            try:
                date.fromisoformat(value)
            except Exception as exc:
                raise ValueError(f"Поле '{field}' должно быть датой в формате YYYY-MM-DD") from exc
        if expected == "string" and fmt == "date-time":
            normalized_value = value.replace("Z", "+00:00") if isinstance(value, str) else value
            try:
                datetime.fromisoformat(normalized_value)
            except Exception as exc:
                raise ValueError(f"Поле '{field}' должно быть датой-временем в ISO-формате") from exc
        enum_values = schema.get("enum")
        if isinstance(enum_values, list) and enum_values:
            if value not in enum_values:
                allowed = "|".join(str(item) for item in enum_values)
                raise ValueError(f"Поле '{field}' должно быть одним из: {allowed}")
        if expected == "number" and not isinstance(value, (int, float)):
            raise ValueError(f"Поле '{field}' должно быть числом")
        if expected == "integer" and not isinstance(value, int):
            raise ValueError(f"Поле '{field}' должно быть целым числом")
        minimum = schema.get("minimum")
        maximum = schema.get("maximum")
        if expected in {"number", "integer"} and isinstance(value, (int, float)):
            if isinstance(minimum, (int, float)) and float(value) < float(minimum):
                raise ValueError(f"Поле '{field}' должно быть >= {minimum}")
            if isinstance(maximum, (int, float)) and float(value) > float(maximum):
                raise ValueError(f"Поле '{field}' должно быть <= {maximum}")
        if expected == "boolean" and not isinstance(value, bool):
            raise ValueError(f"Поле '{field}' должно быть boolean")
        if expected == "array":
            if not isinstance(value, list):
                raise ValueError(f"Поле '{field}' должно быть массивом")
            min_items = schema.get("min_items")
            max_items = schema.get("max_items")
            if isinstance(min_items, int) and len(value) < min_items:
                raise ValueError(f"Поле '{field}' должно содержать не менее {min_items} элементов")
            if isinstance(max_items, int) and len(value) > max_items:
                raise ValueError(f"Поле '{field}' должно содержать не более {max_items} элементов")
            item_schema = schema.get("items")
            if isinstance(item_schema, dict):
                for idx, item in enumerate(value):
                    self._validate_field_type(f"{field}[{idx}]", item, item_schema)
        if expected == "object":
            if not isinstance(value, dict):
                raise ValueError(f"Поле '{field}' должно быть объектом")
            properties = schema.get("properties")
            required = schema.get("required")
            if isinstance(required, list):
                missing = [name for name in required if name not in value or value.get(name) is None]
                if missing:
                    raise ValueError(f"Поле '{field}' должно содержать обязательные ключи: {', '.join(missing)}")
            if isinstance(properties, dict):
                for child_key, child_schema in properties.items():
                    if child_key not in value:
                        continue
                    self._validate_field_type(f"{field}.{child_key}", value[child_key], child_schema)

    @staticmethod
    def _compute_cache_key(
        *,
        provider_id: str,
        model: str,
        analysis_mode: str,
        prompt_template: str,
        expected_json_template: dict[str, Any] | None,
        output_schema: dict[str, Any] | None,
        normalized_review: str,
    ) -> tuple[str, str, str, str, str]:
        """Вычисляет cache_key и вспомогательные хэши для одной строки отчёта.

        Хэши разделены, потому что `cache_key` используется для exact-match кэша в БД,
        а `prompt_hash`/`review_hash`/`expected_template_hash`/`output_schema_hash`
        нужны отдельно для семантического кэша.

        Стабильность:
        - `json.dumps(..., sort_keys=True)` — порядок ключей в schema не влияет
        - Все строки нормализованы перед хэшированием (каждый вызывающий делает `normalize`)
        - SHA-256 — детерминированный, одинаковые входы → одинаковые хэши

        Возвращает кортеж: `(cache_key, prompt_hash, review_hash, expected_template_hash, output_schema_hash)`.
        """
        prompt_hash = hashlib.sha256(prompt_template.encode("utf-8")).hexdigest()
        review_hash = hashlib.sha256(normalized_review.encode("utf-8")).hexdigest()
        expected_template_hash = hashlib.sha256(
            json.dumps(expected_json_template or {}, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        output_schema_hash = hashlib.sha256(
            json.dumps(output_schema or {}, ensure_ascii=False, sort_keys=True).encode("utf-8")
        ).hexdigest()
        cache_key = hashlib.sha256(
            (
                f"{provider_id}\n{model}\n{analysis_mode}\n{prompt_hash}\n"
                f"{expected_template_hash}\n{output_schema_hash}\n{review_hash}"
            ).encode("utf-8")
        ).hexdigest()
        return cache_key, prompt_hash, review_hash, expected_template_hash, output_schema_hash

    @staticmethod
    def _build_summary_from_db(report_id: str) -> dict[str, Any]:
        """SQL-агрегация упрощённого summary (total/success/failed)."""
        agg = get_report_summary_agg(report_id) or {}
        total_rows = int(agg.get("total_rows") or 0)
        success_rows = int(agg.get("success_rows") or 0)
        failed_rows = int(agg.get("failed_rows") or 0)
        return {
            "total_rows": total_rows,
            "processed_rows": total_rows,
            "success_rows": success_rows,
            "failed_rows": failed_rows,
        }

    @staticmethod
    def _get_first_report_row(report_id: str) -> dict[str, Any] | None:
        for row in iter_report_rows(report_id, batch_size=1):
            return row
        return None

    async def _handle_cancel_before_finalize(self, job: Job, *, step: str) -> bool:
        """Проверяет флаг отмены перед тяжёлой финализирующей операцией.

        Возвращает True если отмена была обработана (вызывающий должен вернуться из _run_job).
        Гарантирует что терминальный current_step записан в БД — чтобы не оставалась "Подготовка данных".
        """
        if not job.cancel_event.is_set():
            return False
        job.status = JobStatus.canceled
        job.current_step = "Отменено"
        job.finished_at = datetime.now(timezone.utc)
        job.add_log(f"Обработка отменена пользователем ({step})")
        progress_payload = self._progress_payload(job)
        await asyncio.to_thread(
            update_report_progress,
            report_id=job.report_id,
            total_rows=job.total,
            processed_rows=job.processed,
            progress_percent=float(progress_payload["progress_percent"]),
            eta_seconds=job.eta_seconds,
            current_step=job.current_step,
        )
        await asyncio.to_thread(
            update_report_status,
            report_id=job.report_id,
            status=JobStatus.canceled.value,
            finished_at=job.finished_at.isoformat(),
            current_step=job.current_step,
        )
        logger.info(
            "Job canceled before finalize: job_id=%s step=%s processed=%s total=%s",
            job.id,
            step,
            job.processed,
            job.total,
        )
        await job.emit("done", {"status": job.status.value})
        return True

    def _progress_payload(self, job: Job) -> dict[str, Any]:
        progress = round((job.processed / job.total) * 100, 2) if job.total else 0.0
        return {
            "job_id": job.id,
            "status": job.status.value,
            "total": job.total,
            "processed": job.processed,
            "progress_percent": progress,
            "eta_seconds": job.eta_seconds,
            "current_step": job.current_step,
            "logs": job.logs[-20:],
        }


job_manager = JobManager()
