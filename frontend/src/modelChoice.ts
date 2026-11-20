export type ModelChoice = {
  model: string;
  display_name: string;
  description: string;
  is_default: boolean;
  sort_order: number;
};
export function chooseModel(models: ModelChoice[], saved: string | null) {
  return models.find(m => m.model === saved)?.model
    || models.find(m => m.is_default)?.model || models[0]?.model || "";
}
export function restoreProvider(providers: { id: string }[], savedProvider: string | null,
  savedModel: string | null, choices: ModelChoice[]) {
  if (providers.some(p => p.id === savedProvider) && choices.some(m => m.model === savedModel)) return savedProvider!;
  return providers[0]?.id || "";
}
export function missingModels<T extends { model: string }>(ours: T[], catalog: { model: string }[]) {
  const available = new Set(catalog.map(m => m.model));
  return ours.filter(m => !available.has(m.model));
}
export function partitionMissingModels<T extends { model: string; is_enabled: boolean }>(
  ours: T[], catalog: { model: string }[]
) {
  const missing = missingModels(ours, catalog);
  return { enabled: missing.filter(m => m.is_enabled), disabled: missing.filter(m => !m.is_enabled) };
}
