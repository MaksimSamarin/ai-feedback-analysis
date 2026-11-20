import { uiText, uiLocale } from "./i18n";
export function reliableSinceText(value: string | null | undefined): string {
  const timestamp = value ? Date.parse(value) : NaN;
  if (Number.isNaN(timestamp)) return uiText("Надёжный учёт: дата не определена.");
  const date = new Intl.DateTimeFormat(uiLocale(), {
    day: "2-digit",
    month: "2-digit",
    year: "numeric",
    timeZone: "Europe/Moscow",
  }).format(new Date(timestamp));
  return uiText("Надёжный учёт с {0}.", date);
}
