import { getUiLanguage, uiText, type UILanguage } from "./i18n";
import type {
  AdminStats,
  AuthConfig,
  AuthResponse,
  FileInspectResponse,
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
  if (typeof detail === "object") {
    const message = (detail as Record<string, unknown>).message;
    return typeof message === "string" ? message : JSON.stringify(detail);
  }
  return String(detail);
}

export class APIError extends Error {
  constructor(message: string, public status: number, public detail: Record<string, unknown> | null) { super(uiText(message)); }
}

async function handle<T>(response: Response): Promise<T> {
  if (!response.ok) {
    if (response.status === 413) throw new APIError(
      uiText("Файл превышает допустимый размер загрузки. Уменьшите файл; стандартный лимит — 200 МБ."), 413, null,
    );
    let msg = `${response.status} ${response.statusText}`;
    let detail: Record<string, unknown> | null = null;
    try {
      const body = await response.json();
      msg = normalizeErrorDetail(body.detail) || msg;
      if (body.detail && typeof body.detail === "object") detail = body.detail;
    } catch {
      // ignore
    }
    throw new APIError(msg, response.status, detail);
  }
  return response.json() as Promise<T>;
}

export const api = {
  base: API_BASE,

  async getUiSettings(): Promise<{ language: UILanguage }> {
    const response = await fetch(apiUrl("/ui/settings"), { cache: "no-store", signal: AbortSignal.timeout(3000) });
    const { language } = await handle<{ language: unknown }>(response);
    return { language: language === "ru" ? "ru" : "en" };
  },

  async saveUiSettings(language: UILanguage): Promise<void> {
    await handle(await fetch(apiUrl("/admin/settings"), withCreds({
      method: "PUT",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ language }),
    })));
  },

  async getUserRules(): Promise<{ content_md: string }> {
    return handle(await fetch(apiUrl(`/access/rules?language=${getUiLanguage()}`), withCreds()));
  },

  async bulkDeleteReports(ids: string[]): Promise<{ results: Array<{ id: string; status: string; message?: string }>; deleted: number; failed: number }> {
    return handle(await fetch(apiUrl("/reports/bulk_delete"), withCreds({ method: "POST",
      headers: { "Content-Type": "application/json" }, body: JSON.stringify({ ids }) })));
  },

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

  async getModels(provider: string): Promise<import("./modelChoice").ModelChoice[]> {
    const data = await handle<{ entries: import("./modelChoice").ModelChoice[] }>(await fetch(apiUrl(`/models?provider=${provider}`), withCreds()));
    return data.entries;
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
        body: JSON.stringify({ ...payload, prompt_language: getUiLanguage() }),
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

  async getAuthConfig(): Promise<AuthConfig> {
    return handle<AuthConfig>(await fetch(apiUrl("/auth/config"), withCreds()));
  },

  ssoLoginUrl(force = false): string {
    return apiUrl(force ? "/auth/sso/login?force=1" : "/auth/sso/login");
  },

  async me(): Promise<User> {
    return handle<User>(await fetch(apiUrl("/auth/me"), withCreds()));
  },

  async usage(): Promise<Usage> {
    return handle<Usage>(await fetch(apiUrl("/auth/usage"), withCreds()));
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
      await fetch(apiUrl(`/examples?language=${getUiLanguage()}`), withCreds()),
    );
    return data.examples;
  },

  async downloadExample(name: string): Promise<File> {
    const response = await fetch(
      apiUrl(`/examples/${encodeURIComponent(name)}/download?language=${getUiLanguage()}`),
      withCreds(),
    );
    if (!response.ok) {
      throw new Error(uiText("Не удалось скачать пример: {0}", response.status));
    }
    const blob = await response.blob();
    return new File([blob], name, { type: blob.type });
  },

  async getReleaseNotes(): Promise<ReleaseEntry[]> {
    const data = await handle<{ releases: ReleaseEntry[]; source_missing?: boolean }>(
      await fetch(apiUrl(`/release-notes?language=${getUiLanguage()}`), withCreds()),
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
