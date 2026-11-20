export type Provider = { id: string; label: string };

export type VerifyTokenResult = {
  ok: boolean;
  provider: string;
  status_code: number;
  message: string | null;
  models: string[];
};

export type SheetInfo = {
  name: string;
  columns: string[];
  total_rows: number;
  unique_counts?: Record<string, number | null>;
};

export type FileInspectResponse = {
  file_id: string;
  filename: string;
  sheets: SheetInfo[];
  suggested_sheet: string | null;
  suggested_column: string | null;
  inspect_status: "queued" | "parsing" | "ready" | "error" | string;
  inspect_error_text: string | null;
  queue_position?: number | null;
};


export type JobSummary = {
  total_rows: number;
  processed_rows: number;
  success_rows: number;
  failed_rows: number;
};

export type PreviewRow = {
  row_number: number;
  columns: Record<string, unknown>;
  warnings: string[];
  error: string | null;
};

export type User = {
  id: number;
  username: string;
  full_name?: string | null;
  role: "user" | "admin" | string;
};

export type AuthResponse = {
  token: string;
  username: string;
  role: "user" | "admin" | string;
};

export type AuthConfig = {
  sso_enabled: boolean;
  local_auth_enabled: boolean;
  local_registration_enabled: boolean;
};

export type ReportItem = {
  access_group_id?: number | null;
  access_group_name?: string | null;
  access_group_active?: boolean | null;
  access_group_member?: boolean;
  status_label?: string;
  all_rows_failed?: boolean;
  has_errors?: boolean;
  retry_hint?: string | null;
  failed_rows?: number | null;
  name?: string | null;
  id: string;
  job_id: string;
  status: string;
  provider: string | null;
  model: string | null;
  sheet_name: string | null;
  max_reviews: number | null;
  parallelism: number | null;
  temperature: number | null;
  created_at: string;
  finished_at: string | null;
  updated_at: string | null;
  total_rows: number;
  processed_rows: number;
  progress_percent: number;
  eta_seconds: number | null;
  current_step: string | null;
  uploaded_file_id: string | null;
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  queue_position?: number | null;
  // personal_queue=true → отчёт ждёт освобождения slot'а ВЛАДЕЛЬЦА (превышен
  // MAX_ACTIVE_PER_USER, см. backend/app/db.py). status='queued', но в Redis
  // не пушится. UI рендерит chip как обычно «В очереди», но хинт меняет на
  // «Ждём пока завершится один из ваших активных отчётов и встанем в очередь».
  personal_queue?: boolean;
  results_file: string | null;
  raw_file: string | null;
  summary_json: JobSummary | null;
  error_text: string | null;
  analysis_mode: "custom";
  output_schema_json: Record<string, unknown> | null;
  expected_json_template_json: Record<string, unknown> | null;
  input_columns_json: string[] | null;
  non_analysis_columns_json: string[] | null;
  group_by_column: string | null;
  group_max_rows: number | null;
  source_filename: string | null;
  group_total?: number;
  group_processed?: number;
  // Последние ~20 строк лога обработки (Redis-list job_logs:<id>).
  // Доезжает через polling /api/reports/active каждые ~1.5 сек.
  // После v2.1.0 — единственный канал доставки логов на фронт (SSE удалён).
  last_logs?: string[];
};

export type Usage = {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
  groups?: import("./access").AccessGroup[];
  has_access?: boolean;
};

export type ReportAnalysis = {
  report_id: string;
  status: string;
  summary: JobSummary | null;
  preview_rows: PreviewRow[];
};

export type UserPreset = {
  id: string;
  name: string;
  prompt_template: string;
  expected_json_template: Record<string, unknown>;
  template_hint: string | null;
  created_at: string;
  updated_at: string;
};


export type AdminFailureItem = {
  report_id: string;
  job_id: string;
  user_id: number;
  username: string;
  updated_at: string | null;
  error_text: string | null;
};

export type AdminStats = {
  analysis_workers?: number;
  inspect_workers?: number;
  queue_depth: number;
  queued: number;
  running: number;
  paused: number;
  failed: number;
  recent_failures: AdminFailureItem[];
};
