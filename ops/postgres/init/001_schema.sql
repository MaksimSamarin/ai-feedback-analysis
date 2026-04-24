CREATE TABLE IF NOT EXISTS users (
    id BIGSERIAL PRIMARY KEY,
    username TEXT NOT NULL UNIQUE,
    password_hash TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS sessions (
    token TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    created_at TEXT NOT NULL,
    expires_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS uploaded_files (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    original_name TEXT NOT NULL,
    path TEXT NOT NULL,
    created_at TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS reports (
    id TEXT NOT NULL,
    user_id BIGINT NOT NULL,
    job_id TEXT NOT NULL,
    status TEXT NOT NULL,
    provider TEXT,
    model TEXT,
    sheet_name TEXT,
    column_name TEXT,
    max_reviews INTEGER,
    parallelism INTEGER,
    temperature DOUBLE PRECISION DEFAULT 0,
    created_at TEXT NOT NULL,
    finished_at TEXT,
    updated_at TEXT,
    total_rows INTEGER DEFAULT 0,
    processed_rows INTEGER DEFAULT 0,
    progress_percent DOUBLE PRECISION DEFAULT 0,
    eta_seconds DOUBLE PRECISION,
    current_step TEXT,
    uploaded_file_id TEXT,
    prompt_template TEXT,
    include_raw_json INTEGER DEFAULT 1,
    api_key_encrypted TEXT,
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    results_file TEXT,
    raw_file TEXT,
    summary_json TEXT,
    error_text TEXT,
    analysis_mode TEXT DEFAULT 'custom',
    output_schema_json TEXT,
    expected_json_template_json TEXT,
    input_columns_json TEXT,
    use_cache INTEGER DEFAULT 1,
    PRIMARY KEY (id, user_id)
) PARTITION BY HASH (user_id);


CREATE TABLE IF NOT EXISTS report_rows (
    report_id TEXT NOT NULL,
    row_number INTEGER NOT NULL,
    review_text TEXT,
    warnings_json TEXT,
    status TEXT NOT NULL,
    sentiment_label TEXT,
    negativity_score DOUBLE PRECISION,
    short_reason TEXT,
    key_topics_json TEXT,
    raw_response_json TEXT,
    input_json TEXT,
    custom_json TEXT,
    error_text TEXT,
    prompt_tokens INTEGER DEFAULT 0,
    completion_tokens INTEGER DEFAULT 0,
    total_tokens INTEGER DEFAULT 0,
    PRIMARY KEY (report_id, row_number)
) PARTITION BY HASH (report_id);


CREATE TABLE IF NOT EXISTS llm_cache (
    cache_key TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    review_hash TEXT NOT NULL,
    analysis_json TEXT NOT NULL,
    raw_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    hits INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS llm_semantic_cache (
    semantic_key TEXT PRIMARY KEY,
    provider TEXT NOT NULL,
    model TEXT NOT NULL,
    analysis_mode TEXT NOT NULL,
    prompt_hash TEXT NOT NULL,
    expected_template_hash TEXT NOT NULL,
    output_schema_hash TEXT NOT NULL,
    embedding_json TEXT NOT NULL,
    analysis_json TEXT NOT NULL,
    raw_json TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    hits INTEGER DEFAULT 0
);

CREATE TABLE IF NOT EXISTS user_presets (
    id TEXT PRIMARY KEY,
    user_id BIGINT NOT NULL,
    name TEXT NOT NULL,
    prompt_template TEXT NOT NULL,
    expected_json_template_json TEXT NOT NULL,
    template_hint TEXT,
    created_at TEXT NOT NULL,
    updated_at TEXT NOT NULL,
    UNIQUE(user_id, name)
);

CREATE INDEX IF NOT EXISTS idx_reports_id ON reports (id);
CREATE INDEX IF NOT EXISTS idx_reports_job_id_user ON reports (job_id, user_id);
CREATE INDEX IF NOT EXISTS idx_reports_user_created_at ON reports (user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_report_rows_report_row_status ON report_rows (report_id, row_number, status);
CREATE INDEX IF NOT EXISTS idx_semantic_cache_lookup ON llm_semantic_cache (provider, model, analysis_mode, prompt_hash, expected_template_hash, output_schema_hash, updated_at DESC);


ALTER TABLE llm_cache SET (
    autovacuum_vacuum_scale_factor = 0.03,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_threshold = 500
);

ALTER TABLE llm_semantic_cache SET (
    autovacuum_vacuum_scale_factor = 0.03,
    autovacuum_analyze_scale_factor = 0.01,
    autovacuum_vacuum_threshold = 1000,
    autovacuum_analyze_threshold = 500
);
