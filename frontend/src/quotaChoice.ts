import { uiGroupName, uiText, uiLocale } from "./i18n";
export type Quota = {
  id: number;
  remaining: number | null
};
export const quotaAvailable = (group: Quota) => group.remaining == null || group.remaining > 0;
export function chooseQuota(groups: Quota[], lastId?: number | null): number | "" {
  const available = groups.filter(quotaAvailable);
  return available.find(g => g.id === lastId)?.id || available[0]?.id || "";
}
export function retainQuotaSelection(previous: Quota[] | null, next: Quota[], selected: number | "", lastId?: number | null): number | "" {
  if (next.some(g => g.id === selected)) return selected;
  if (previous === null) return chooseQuota(next, lastId);
  const known = new Set(previous.map(g => g.id));
  return chooseQuota(next.filter(g => known.has(g.id)), lastId);
}
export type QuotaDisplay = Quota & {
  name: string;
  used: number;
  token_limit: number | null;
  limit_period: string;
  reset_at: string | null;
  group_remaining?: number | null;
  limit_mode?: string;
  personal_used?: number;
  personal_token_limit?: number | null;
  personal_remaining?: number | null;
};
const compact = (value: number) => new Intl.NumberFormat(uiLocale(), {
  notation: "compact", maximumFractionDigits: 1,
}).format(value);
const full = (value: number) => value.toLocaleString(uiLocale());
export function quotaReset(value: string | null, long = false) {
  return value ? new Intl.DateTimeFormat(
    uiLocale(),
    {
      timeZone: "Europe/Moscow", day: "numeric", month: long ? "long" : "short",
    }
  ).format(new Date(value)) : null;
}
export function quotaFieldName(groups: Array<Quota & {
  name: string
}>, selected: number | "") {
  return groups.find(g => g.id === selected)?.name || (groups.some(quotaAvailable) ? uiText("Выберите квоту") : uiText("Нет доступной квоты"));
}
export function quotaMenuBalance(group: Quota & {
  reset_at: string | null
}) {
  if (quotaAvailable(group)) return group.remaining == null ? uiText("без лимита") : uiText("осталось {0}", compact(group.remaining));
  const reset = quotaReset(group.reset_at);
  return uiText("исчерпан · {0}", reset ? uiText("сброс {0}", reset) : uiText("без сброса"));
}
function scale(used: number, limit: number | null, remaining: number | null, prefix: string, suffix = "") {
  if (limit === null) return {
    line: uiText("Без лимита · израсходовано {0}{1}", compact(used), suffix),
    tooltip: uiText("Без лимита · израсходовано {0} токенов{1}", full(used), suffix),
    progress: null,
  };
  const left = Math.max(0, remaining ?? limit - used);
  return {
    line: uiText("{0}{1} из {2} · осталось {3}", prefix, compact(used), compact(limit), compact(left)),
    tooltip: uiText("{0}{1} из {2} токенов · осталось {3}", prefix, full(used), full(limit), full(left)),
    progress: limit <= 0 ? 100 : Math.max(0, Math.min(100, used / limit * 100)),
  };
}
export function quotaSummary(group: QuotaDisplay) {
  const reset = quotaReset(group.reset_at, true);
  const groupScale = scale(
    group.used,
    group.token_limit,
    group.group_remaining ?? null,
    uiText("Израсходовано") + " ",
    group.limit_period === "month" ? " " + uiText("в этом месяце") : ""
  );
  if (group.token_limit !== null && reset) {
    groupScale.line += uiText("· сброс {0}", reset);
    groupScale.tooltip += " " + uiText("· сброс") + " " + new Intl.DateTimeFormat(
      uiLocale(),
      {
        timeZone: "Europe/Moscow",
        day: "2-digit",
        month: "2-digit",
        year: "numeric",
        hour: "2-digit",
        minute: "2-digit",
      }
    ).format(new Date(group.reset_at!)) + " " + uiText("МСК");
  }
  return {
    group: groupScale,
    personal: group.limit_mode === "personal" ? scale(
      group.personal_used ?? 0,
      group.personal_token_limit ?? 0,
      group.personal_remaining ?? null,
      uiText("Ваш личный лимит:") + " "
    ) : null,
  };
}
export function quotaScaleColor(progress: number | null, dark: boolean) {
  return progress !== null && progress >= 100 ? (dark ? "#f87171" : "#b91c1c")
    : progress !== null && progress >= 80 ? (dark ? "#f6c56f" : "#a56508") : (dark ? "#5ad6aa" : "#16835b");
}
export function quotaBlockedReason(groups: Array<Quota & {
  name: string;
  reset_at: string | null
}>, selected: number | "") {
  if (!groups.length) return null;
  if (!groups.some(quotaAvailable)) return uiText("Запуск недоступен: все квоты исчерпаны.") + " " +
    groups.map(g => `${uiGroupName(g.name)} — ${quotaMenuBalance(g)}`).join("; ");
  if (groups.some(g => g.id === selected && quotaAvailable(g))) return null;
  return uiText("Выберите группу с доступным остатком перед запуском.");
}
