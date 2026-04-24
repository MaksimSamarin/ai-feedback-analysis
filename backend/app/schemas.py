from __future__ import annotations

from datetime import datetime
from enum import Enum
from typing import Any

from pydantic import BaseModel, Field, field_validator
from app.config import GLOBAL_LLM_PARALLELISM


class SentimentLabel(str, Enum):
    negative = "negative"
    neutral = "neutral"
    positive = "positive"


class AnalysisOutput(BaseModel):
    category: str = Field(min_length=1, max_length=120)
    sentiment_label: SentimentLabel
    negativity_score: float = Field(ge=0.0, le=1.0)
    summary: str = Field(min_length=1, max_length=240)


class SheetInfo(BaseModel):
    name: str
    columns: list[str]
    total_rows: int = 0
    unique_counts: dict[str, int | None] = {}


class FileInspectResponse(BaseModel):
    file_id: str
    filename: str
    sheets: list[SheetInfo]
    suggested_sheet: str | None
    suggested_column: str | None
    inspect_status: str = "ready"
    inspect_error_text: str | None = None
    queue_position: int | None = None


class AuthRequest(BaseModel):
    username: str = Field(min_length=3, max_length=64)
    password: str = Field(min_length=6, max_length=128)


class AuthResponse(BaseModel):
    token: str
    username: str
    role: str = "user"


class StartJobRequest(BaseModel):
    file_id: str
    sheet_name: str
    analysis_columns: list[str] = Field(min_length=1)
    non_analysis_columns: list[str] | None = None
    group_by_column: str | None = None
    provider: str
    model: str
    api_key: str | None = None
    prompt_template: str
    max_reviews: int = Field(default=100, ge=1, le=1000000)
    parallelism: int = Field(default=3, ge=1)
    temperature: float = Field(default=0.0, ge=0.0, le=2.0)
    save_api_key_for_resume: bool = False
    use_cache: bool = True
    include_raw_json: bool = True
    analysis_mode: str = Field(default="custom", pattern="^custom$")
    output_schema: dict[str, Any] | None = None
    expected_json_template: dict[str, Any] | None = None

    @field_validator("parallelism")
    @classmethod
    def validate_parallelism(cls, value: int) -> int:
        max_parallelism = max(1, GLOBAL_LLM_PARALLELISM)
        if value > max_parallelism:
            raise ValueError(f"parallelism must be <= {max_parallelism}")
        return value


class JobStatus(str, Enum):
    queued = "queued"
    running = "running"
    paused = "paused"
    completed = "completed"
    failed = "failed"
    canceled = "canceled"


class JobSummary(BaseModel):
    total_rows: int
    processed_rows: int
    success_rows: int
    failed_rows: int


class PreviewRow(BaseModel):
    row_number: int
    columns: dict[str, Any] = Field(default_factory=dict)
    warnings: list[str] = Field(default_factory=list)
    error: str | None = None


class JobResult(BaseModel):
    summary: JobSummary | None = None
    preview_rows: list[PreviewRow] = Field(default_factory=list)
    results_file: str | None = None
    raw_file: str | None = None


class JobStateResponse(BaseModel):
    job_id: str
    status: JobStatus
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    total: int
    processed: int
    progress_percent: float
    eta_seconds: float | None
    current_step: str
    logs: list[str]
    result: JobResult
    queue_position: int | None = None


class ProviderInfo(BaseModel):
    id: str
    label: str


class ProvidersResponse(BaseModel):
    providers: list[ProviderInfo]


class ModelsResponse(BaseModel):
    provider: str
    models: list[str]


class VerifyTokenRequest(BaseModel):
    provider: str
    api_key: str | None = None


class VerifyTokenResponse(BaseModel):
    ok: bool
    provider: str
    status_code: int
    message: str | None = None
    models: list[str] = Field(default_factory=list)


class EventMessage(BaseModel):
    type: str
    payload: dict[str, Any]


class ReportItem(BaseModel):
    id: str
    job_id: str
    status: str
    provider: str | None = None
    model: str | None = None
    sheet_name: str | None = None
    max_reviews: int | None = None
    parallelism: int | None = None
    temperature: float | None = None
    created_at: str
    finished_at: str | None = None
    updated_at: str | None = None
    total_rows: int = 0
    processed_rows: int = 0
    progress_percent: float = 0
    eta_seconds: float | None = None
    current_step: str | None = None
    uploaded_file_id: str | None = None
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    queue_position: int | None = None
    results_file: str | None = None
    raw_file: str | None = None
    summary_json: dict[str, Any] | None = None
    error_text: str | None = None
    analysis_mode: str | None = "custom"
    output_schema_json: dict[str, Any] | None = None
    expected_json_template_json: dict[str, Any] | None = None
    input_columns_json: list[str] | None = None
    non_analysis_columns_json: list[str] | None = None
    group_by_column: str | None = None
    group_max_rows: int | None = None
    # Имя исходного файла без расширения (из uploaded_files.original_name).
    # Используется на главной странице как «название отчёта» первой колонкой.
    source_filename: str | None = None
    # Счётчики по группам для отчётов с group_by_column: сколько всего групп
    # и сколько уже обработано (хотя бы одна строка группы в статусе done/error).
    # Для обычных отчётов без группировки оба поля = 0.
    group_total: int = 0
    group_processed: int = 0


class ReportsResponse(BaseModel):
    reports: list[ReportItem]


class ReportAnalysisResponse(BaseModel):
    report_id: str
    status: str
    summary: JobSummary | None = None
    preview_rows: list[PreviewRow] = Field(default_factory=list)


class UsageResponse(BaseModel):
    prompt_tokens: int
    completion_tokens: int
    total_tokens: int


class UserMeResponse(BaseModel):
    id: int
    username: str
    role: str = "user"


class PresetUpsertRequest(BaseModel):
    name: str = Field(min_length=1, max_length=80)
    prompt_template: str = Field(min_length=1)
    expected_json_template: dict[str, Any]
    template_hint: str | None = Field(default=None, max_length=40)


class PresetItem(BaseModel):
    id: str
    name: str
    prompt_template: str
    expected_json_template: dict[str, Any]
    template_hint: str | None = None
    created_at: str
    updated_at: str


class PresetsResponse(BaseModel):
    presets: list[PresetItem]


class AdminUserItem(BaseModel):
    id: int
    username: str
    role: str
    created_at: str
    reports_count: int = 0
    last_login_at: str | None = None


class AdminUsersResponse(BaseModel):
    users: list[AdminUserItem]


class AdminFailureItem(BaseModel):
    report_id: str
    job_id: str
    user_id: int
    username: str
    updated_at: str | None = None
    error_text: str | None = None


class AdminStatsResponse(BaseModel):
    queue_depth: int = 0
    queued: int = 0
    running: int = 0
    paused: int = 0
    failed: int = 0
    recent_failures: list[AdminFailureItem] = Field(default_factory=list)


class AdminLogItem(BaseModel):
    ts: str | None = None
    level: str | None = None
    service: str | None = None
    logger: str | None = None
    request_id: str | None = None
    user_id: str | None = None
    username: str | None = None
    message: str
    raw: str


class AdminLogsResponse(BaseModel):
    service: str
    lines: list[AdminLogItem] = Field(default_factory=list)
