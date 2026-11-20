import type { RequestNotice } from "./requestPresentation";
type NoticeStorage = Pick<Storage, "getItem" | "setItem">;
export function browserNoticeStorage(): NoticeStorage | undefined {
  try { return typeof window === "undefined" ? undefined : window.localStorage; } catch { return undefined; }
}
export function createRequestNoticeTracker(storage?: NoticeStorage) {
  const pending = new Set<number>(), dismissed = new Set<number>();
  const key = (id: number) => `review_analyzer_request_notice:${id}`;
  function read(id: number) {
    try { return storage?.getItem(key(id)); } catch { return null; }
  }
  function write(id: number, value: string) {
    try { storage?.setItem(key(id), value); } catch { /* Состояние текущей вкладки остаётся в памяти. */ }
  }
  return {
    observe(request?: RequestNotice) {
      if (request?.status === "pending") {
        pending.add(request.id);
        if (!dismissed.has(request.id) && read(request.id) !== "dismissed") write(request.id, "pending");
      }
    },
    state(request?: RequestNotice) {
      if (!request) return { dismissed: false, approved: false };
      const saved = read(request.id);
      const closed = dismissed.has(request.id) || saved === "dismissed";
      return {
        dismissed: closed,
        approved: !closed && request.status === "approved" && (pending.has(request.id) || saved === "pending")
      };
    },
    dismiss(id: number) {
      dismissed.add(id);
      write(id, "dismissed");
    },
  };
}
export function rememberPendingRequests(requests: RequestNotice[], storage = browserNoticeStorage()) {
  const tracker = createRequestNoticeTracker(storage);
  requests.forEach(request => tracker.observe(request));
}
