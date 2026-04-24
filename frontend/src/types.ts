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

export type JobStatus = "queued" | "running" | "paused" | "completed" | "failed" | "canceled";

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

export type JobState = {
  job_id: string;
  status: JobStatus;
  created_at: string;
  started_at: string | null;
  finished_at: string | null;
  total: number;
  processed: number;
  progress_percent: number;
  eta_seconds: number | null;
  current_step: string;
  logs: string[];
  result: {
    summary: JobSummary | null;
    preview_rows: PreviewRow[];
    results_file: string | null;
    raw_file: string | null;
  };
  queue_position?: number | null;
};

export type SseEvent = {
  type: string;
  payload: Record<string, unknown>;
};

export type User = {
  id: number;
  username: string;
  role: "user" | "admin" | string;
};

export type AuthResponse = {
  token: string;
  username: string;
  role: "user" | "admin" | string;
};

export type ReportItem = {
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
};

export type Usage = {
  prompt_tokens: number;
  completion_tokens: number;
  total_tokens: number;
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

export type AdminUserItem = {
  id: number;
  username: string;
  role: string;
  created_at: string;
  reports_count: number;
  last_login_at: string | null;
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
  queue_depth: number;
  queued: number;
  running: number;
  paused: number;
  failed: number;
  recent_failures: AdminFailureItem[];
};

export type AdminLogItem = {
  ts: string | null;
  level: string | null;
  service: string | null;
  logger: string | null;
  request_id: string | null;
  user_id: string | null;
  username: string | null;
  message: string;
  raw: string;
};
