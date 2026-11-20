import english from "./locale/en.json";

export type UILanguage = "en" | "ru";

const englishText: Readonly<Record<string, string>> = english;
let interfaceLanguage: UILanguage = "en";

export function setUiLanguage(language: UILanguage) {
  interfaceLanguage = language;
  document.documentElement.lang = language;
}

export function getUiLanguage(): UILanguage {
  return interfaceLanguage;
}

export function uiLocale(): string {
  return interfaceLanguage === "en" ? "en-GB" : "ru-RU";
}

export function uiText(source: string, ...values: unknown[]): string {
  const translated = interfaceLanguage === "en" && Object.prototype.hasOwnProperty.call(englishText, source)
    ? englishText[source]
    : source;
  return values.length
    ? translated.replace(/\{(\d+)\}/g, (placeholder, index) =>
      Number(index) < values.length ? String(values[Number(index)] ?? "") : placeholder)
    : translated;
}

export function uiGroupName(name: string): string {
  return name === "Гость" ? uiText(name) : name;
}
