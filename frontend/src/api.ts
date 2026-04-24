import type {
  AdminLogItem,
  AdminStats,
  AdminUserItem,
  AuthResponse,
  FileInspectResponse,
  JobState,
  Provider,
  ReportAnalysis,
  ReportItem,
  Usage,
  User,
  UserPreset,
  VerifyTokenResult,
} from "./types";

const RAW_API_BASE = import.meta.env.VITE_API_BASE || "http://localhost:8000";
const API_BASE = RAW_API_BASE.replace(/\/+$/, "");
const API_PREFIX = API_BASE.endsWith("/api") ? "" : "/api";

function apiUrl(path: string): string {
  return `${API_BASE}${API_PREFIX}${path}`;
}

function withCreds(init: RequestInit = {}): RequestInit {
  return {
    ...init,
    credentials: "include",
  };
}

function normalizeErrorDetail(detail: unknown): string {
  if (!detail) return "";
  if (typeof detail === "string") return detail;
  if (Array.isArray(detail)) {
    const parts = detail
      .map((item) => {
        if (typeof item === "string") return item;
        if (item && typeof item === "object") {
          const obj = item as Record<string, unknown>;
          const msg = typeof obj.msg === "string" ? obj.msg : JSON.stringify(obj);
          const loc = Array.isArray(obj.loc) ? obj.loc.join(".") : "";
          return loc ? `${loc}: ${msg}` : msg;
        }
        return String(item);
      })
      .filter(Boolean);
    return parts.join("; ");
  }
  if (typeof detail === "object") return JSON.stringify(detail);
  return String(detail);
}

async function handle<T>(response: Response): Promise<T> {
  if (!response.ok) {
    let msg = `${response.status} ${response.statusText}`;
    try {
      const body = await response.json();
      msg = normalizeErrorDetail(body.detail) || msg;
    } catch {
      // ignore
    }
    throw new Error(msg);
  }
  return response.json() as Promise<T>;
}

export const api = {
  base: API_BASE,

  async getDefaultPrompt(): Promise<{ promptTemplate: string; parallelismMax: number }> {
    const data = await handle<{ prompt_template: string; parallelism_max?: number }>(
      await fetch(apiUrl("/default-prompt"), withCreds()),
    );
    return {
      promptTemplate: data.prompt_template,
      parallelismMax: Math.max(1, Number(data.parallelism_max ?? 20)),
    };
  },

  async getProviders(): Promise<Provider[]> {
    const data = await handle<{ providers: Provider[] }>(await fetch(apiUrl("/providers"), withCreds()));
    return data.providers;
  },

  async getModels(provider: string): Promise<string[]> {
    const data = await handle<{ models: string[] }>(await fetch(apiUrl(`/models?provider=${provider}`), withCreds()));
    return data.models;
  },

  async verifyProviderToken(provider: string, apiKey: string | null): Promise<VerifyTokenResult> {
    return handle<VerifyTokenResult>(
      await fetch(apiUrl("/providers/verify-token"), withCreds({
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ provider, api_key: apiKey }),
      })),
    );
  },

  async inspectFile(file: File): Promise<FileInspectResponse> {
    const form = new FormData();
    form.append("file", file);
    const response = await fetch(apiUrl("/file/inspect"), withCreds({ method: "POST", body: form }));
    return handle<FileInspectResponse>(response);
  },

  async getFileInspect(fileId: string): Promise<FileInspectResponse> {
    return handle<FileInspectResponse>(await fetch(apiUrl(`/file/${encodeURIComponent(fileId)}/inspect`), withCreds()));
  },

  async startJob(payload: Record<string, unknown>): Promise<string> {
    const data = await handle<{ job_id: string }>(
      await fetch(apiUrl("/jobs"), withCreds({
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })),
    );
    return data.job_id;
  },

  async cancelJob(jobId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/jobs/${jobId}/cancel`), withCreds({ method: "POST" })));
  },

  async pauseJob(jobId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/jobs/${jobId}/pause`), withCreds({ method: "POST" })));
  },

  async resumeJob(jobId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/jobs/${jobId}/resume`), withCreds({ method: "POST" })));
  },

  async retryJob(jobId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/jobs/${jobId}/retry`), withCreds({ method: "POST" })));
  },

  async getJob(jobId: string): Promise<JobState> {
    return handle<JobState>(await fetch(apiUrl(`/jobs/${jobId}`), withCreds()));
  },

  jobEventsUrl(jobId: string): string {
    return apiUrl(`/jobs/${jobId}/events`);
  },

  async register(username: string, password: string): Promise<AuthResponse> {
    return handle<AuthResponse>(
      await fetch(apiUrl("/auth/register"), withCreds({
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      })),
    );
  },

  async login(username: string, password: string): Promise<AuthResponse> {
    return handle<AuthResponse>(
      await fetch(apiUrl("/auth/login"), withCreds({
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ username, password }),
      })),
    );
  },

  async me(): Promise<User> {
    return handle<User>(await fetch(apiUrl("/auth/me"), withCreds()));
  },

  async usage(): Promise<Usage> {
    return handle<Usage>(await fetch(apiUrl("/auth/usage"), withCreds()));
  },

  async adminUsers(): Promise<AdminUserItem[]> {
    const data = await handle<{ users: AdminUserItem[] }>(await fetch(apiUrl("/admin/users"), withCreds()));
    return data.users;
  },

  async adminUserReports(userId: number, limit = 50): Promise<ReportItem[]> {
    const data = await handle<{ reports: ReportItem[] }>(
      await fetch(apiUrl(`/admin/users/${userId}/reports?limit=${limit}`), withCreds()),
    );
    return data.reports;
  },

  async adminReportAnalysis(reportId: string): Promise<ReportAnalysis> {
    return handle<ReportAnalysis>(await fetch(apiUrl(`/admin/reports/${reportId}/analysis`), withCreds()));
  },

  async adminPauseReport(reportId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/admin/reports/${reportId}/pause`), withCreds({ method: "POST" })));
  },

  async adminResumeReport(reportId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/admin/reports/${reportId}/resume`), withCreds({ method: "POST" })));
  },

  async adminCancelReport(reportId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/admin/reports/${reportId}/cancel`), withCreds({ method: "POST" })));
  },

  async adminDeleteReport(reportId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/admin/reports/${reportId}`), withCreds({ method: "DELETE" })));
  },

  async adminStats(): Promise<AdminStats> {
    return handle<AdminStats>(await fetch(apiUrl("/admin/stats"), withCreds()));
  },

  async adminLogs(service: "all" | "backend" | "worker" = "all", limit = 200, level?: string, q?: string): Promise<AdminLogItem[]> {
    const params = new URLSearchParams();
    params.set("service", service);
    params.set("limit", String(limit));
    if (level && level.trim()) params.set("level", level.trim());
    if (q && q.trim()) params.set("q", q.trim());
    const data = await handle<{ service: string; lines: AdminLogItem[] }>(
      await fetch(apiUrl(`/admin/logs?${params.toString()}`), withCreds()),
    );
    return data.lines || [];
  },

  adminReportDownloadUrl(reportId: string, kind: "xlsx" | "raw" | "source"): string {
    return apiUrl(`/admin/reports/${reportId}/download/${kind}`);
  },

  async logout(): Promise<void> {
    await handle(await fetch(apiUrl("/auth/logout"), withCreds({ method: "POST" })));
  },

  async getReports(): Promise<ReportItem[]> {
    const data = await handle<{ reports: ReportItem[] }>(await fetch(apiUrl("/reports"), withCreds()));
    return data.reports;
  },

  async getActiveReports(): Promise<ReportItem[]> {
    const data = await handle<{ reports: ReportItem[] }>(await fetch(apiUrl("/reports/active"), withCreds()));
    return data.reports;
  },

  async getReportAnalysis(reportId: string): Promise<ReportAnalysis> {
    return handle<ReportAnalysis>(await fetch(apiUrl(`/reports/${reportId}/analysis`), withCreds()));
  },

  async deleteReport(reportId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/reports/${reportId}`), withCreds({ method: "DELETE" })));
  },

  async listPresets(): Promise<UserPreset[]> {
    const data = await handle<{ presets: UserPreset[] }>(await fetch(apiUrl("/presets"), withCreds()));
    return data.presets;
  },

  async savePreset(payload: {
    name: string;
    prompt_template: string;
    expected_json_template: Record<string, unknown>;
    template_hint?: string | null;
  }): Promise<UserPreset> {
    return handle<UserPreset>(
      await fetch(apiUrl("/presets"), withCreds({
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      })),
    );
  },

  async deletePreset(presetId: string): Promise<void> {
    await handle(await fetch(apiUrl(`/presets/${presetId}`), withCreds({ method: "DELETE" })));
  },

  async listExamples(): Promise<ExampleFile[]> {
    const data = await handle<{ examples: ExampleFile[] }>(
      await fetch(apiUrl("/examples"), withCreds()),
    );
    return data.examples;
  },

  async downloadExample(name: string): Promise<File> {
    const response = await fetch(
      apiUrl(`/examples/${encodeURIComponent(name)}/download`),
      withCreds(),
    );
    if (!response.ok) {
      throw new Error(`Не удалось скачать пример: ${response.status}`);
    }
    const blob = await response.blob();
    return new File([blob], name, { type: blob.type });
  },

  async getReleaseNotes(): Promise<ReleaseEntry[]> {
    const data = await handle<{ releases: ReleaseEntry[]; source_missing?: boolean }>(
      await fetch(apiUrl("/release-notes"), withCreds()),
    );
    return data.releases || [];
  },
};

export type ExampleFile = {
  name: string;
  size_bytes: number;
};

export type ReleaseEntry = {
  version: string;
  title: string;
  content_md: string;
};
