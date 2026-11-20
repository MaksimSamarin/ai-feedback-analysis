"""Индексы периода и безопасная витрина для внешней отчётности."""
from app import db


def init_schema(conn):
    conn.execute('CREATE INDEX IF NOT EXISTS idx_reports_created_at ON reports(created_at)')
    conn.execute("""CREATE OR REPLACE VIEW reporting_usage_daily AS
        SELECT (r.created_at::timestamptz AT TIME ZONE 'Europe/Moscow')::date AS day_msk,
        date_trunc('month',r.created_at::timestamptz AT TIME ZONE 'Europe/Moscow')::date AS month_msk,
        r.access_group_id AS group_id,COALESCE(g.name,'Без группы') AS group_name,
        r.user_id,u.username,u.full_name,r.provider,r.model,
        CASE WHEN COALESCE(r.group_by_column,'')<>'' THEN 'grouped' ELSE 'rows' END AS mode,r.status,
        COUNT(*) AS reports,COALESCE(SUM(r.processed_rows),0) AS processed_rows,
        SUM(r.failed_rows) AS failed_rows,COALESCE(SUM(r.prompt_tokens),0) AS prompt_tokens,
        COALESCE(SUM(r.completion_tokens),0) AS completion_tokens,COALESCE(SUM(r.total_tokens),0) AS total_tokens,
        COALESCE(SUM(r.limit_pause_count),0) AS limit_stops,COUNT(*) FILTER(WHERE r.deleted_at IS NOT NULL) AS deleted_reports
        FROM reports r JOIN users u ON u.id=r.user_id LEFT JOIN access_groups g ON g.id=r.access_group_id
        GROUP BY day_msk,month_msk,r.access_group_id,g.name,r.user_id,u.username,u.full_name,r.provider,r.model,mode,r.status""")
    conn.execute("INSERT INTO schema_migrations(name,applied_at) VALUES ('reporting_sql_v2_2_0',?) ON CONFLICT(name) DO NOTHING",(db._now_iso(),))
