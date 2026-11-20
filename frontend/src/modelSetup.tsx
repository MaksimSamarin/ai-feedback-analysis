import { uiText } from "./i18n";
import { Alert, AlertTitle, Button, CircularProgress } from "@mui/material";
import { accessCall } from "./access";
import { navigate } from "./adminShared";
export const EMPTY_MODELS_TEXT = uiText("Модели ещё не настроены. Обратитесь к администратору");
export function ModelSetupNotice({ isAdmin }: { isAdmin: boolean }) {
  return <Alert severity={isAdmin ? "warning" : "info"}
    action={isAdmin ? <Button color="inherit" onClick={() => navigate("#/admin/models")}>{uiText("К моделям")}</Button> : undefined}>
    {isAdmin ? <><AlertTitle>{uiText("Модели не настроены")}</AlertTitle>{uiText("Добавьте модели в разделе «Модели», чтобы запускать отчёты.")}</> : EMPTY_MODELS_TEXT}
  </Alert>;
}
export type CatalogChoice = { model: string; already_added: boolean; is_chat_candidate?: boolean; hidden_reason?: string | null };
export function visibleCatalog(rows: CatalogChoice[], showAll: boolean, query: string) {
  return rows.filter(r => (showAll || r.is_chat_candidate !== false) && r.model.toLowerCase().includes(query.toLowerCase()));
}
export function toggleModelSelection(selected: string[], row: CatalogChoice) {
  if (row.already_added) return selected;
  return selected.includes(row.model) ? selected.filter(n => n !== row.model) : [...selected, row.model];
}
export const MODEL_BATCH_LIMIT = 20;
export type BatchModelsResult = { results: { created: boolean; error?: string }[] };
export async function submitModelSelection(provider: string, selected: string[], call = accessCall) {
  if (!selected.length || selected.length > MODEL_BATCH_LIMIT) throw new Error(uiText("Выберите от 1 до 20 моделей"));
  return call<BatchModelsResult>("/admin/models/batch", "POST", { provider, models: selected });
}
export function BatchModelButton({ count, busy, onClick }: { count: number; busy: boolean; onClick: () => void }) {
  return <Button variant="contained" disabled={busy || !count || count > MODEL_BATCH_LIMIT} onClick={onClick}
    startIcon={busy ? <CircularProgress size={16} color="inherit" /> : undefined}>{uiText("Добавить выбранные (")}{count})
  </Button>;
}
