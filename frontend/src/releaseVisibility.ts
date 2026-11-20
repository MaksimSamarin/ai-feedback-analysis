import { uiText } from "./i18n";
export function shouldAutoShowRelease(signedIn: boolean, hasAccess: boolean, open: boolean, seen: string | null, current: string) {
  return signedIn && hasAccess && !open && seen !== current;
}
export function releaseNotesErrorMessage(status?: number) {
  return status === 401 ? uiText("Время входа истекло. Войдите снова, чтобы прочитать заметки.")
    : uiText("Не удалось загрузить заметки о релизе. Попробуйте позже.");
}
