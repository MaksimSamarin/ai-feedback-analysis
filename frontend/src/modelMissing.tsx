import { uiText } from "./i18n";
import { Alert, Box, Button, Stack, Typography } from "@mui/material";
import { partitionMissingModels } from "./modelChoice";

export type MissingModelRow = {
  id: number;
  model: string;
  display_name: string;
  is_enabled: boolean;
  is_default: boolean;
  is_used?: boolean;
};

export function MissingModelNotices({ models, catalog, busy, onDisable, onDelete }: {
  models: MissingModelRow[];
  catalog: { model: string }[];
  busy: boolean;
  onDisable: (id: number) => void;
  onDelete: (id: number) => void;
}) {
  const { enabled, disabled } = partitionMissingModels(models, catalog);
  const name = (row: MissingModelRow) => row.display_name
    + (row.display_name !== row.model ? ` (${row.model})` : "");
  return <>
    {enabled.length > 0 && <Alert severity="warning" sx={{ mb: 2 }}>{uiText("Эти модели есть в списке, но отсутствуют у провайдера:")}{enabled.map(row => <Stack key={row.id} direction="row" alignItems="center" gap={1}
        flexWrap="wrap" sx={{ overflowWrap: "anywhere" }}>
        <Typography variant="body2">{name(row)}</Typography>
        <Button size="small" color="inherit" disabled={busy || row.is_default}
          onClick={() => onDisable(row.id)}>{uiText("Выключить")}</Button>
        {row.is_default && <Typography variant="caption">{uiText("Сначала выберите новую основную")}</Typography>}
      </Stack>)}
    </Alert>}
    {disabled.length > 0 && <Box sx={{ mb: 2, minWidth: 0 }}>
      <Typography component="div" variant="body2" color="text.secondary"
        sx={{ overflowWrap: "anywhere", lineHeight: 1.8 }}>{uiText("Выключены и отсутствуют у провайдера:")}{" "}
        {disabled.map((row, index) => <Box component="span" key={row.id}>
          {index > 0 && "; "}{name(row)}
          {row.is_used === false && <Button size="small" color="error" disabled={busy}
            sx={{ ml: .5 }} onClick={() => onDelete(row.id)}>{uiText("Удалить")}</Button>}
        </Box>)}
      </Typography>
    </Box>}
  </>;
}
