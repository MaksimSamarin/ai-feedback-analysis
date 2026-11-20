import { uiGroupName, uiText } from "./i18n";
import type { ReactNode } from "react";
import {
  Box,
  Button,
  LinearProgress,
  MenuItem,
  Stack,
  TextField,
  Tooltip,
  Typography,
  useTheme
} from "@mui/material";
import type { AccessGroup } from "./access";
import { quotaAvailable, quotaFieldName, quotaMenuBalance, quotaSummary, quotaScaleColor } from "./quotaChoice";
export function QuotaMenuLabel({ group }: {
  group: AccessGroup
}) {
  return <Box
    sx={{
      display: "flex",
      alignItems: "baseline",
      justifyContent: "space-between",
      gap: 2,
      width: "100%",
      minWidth: 0
    }}
  >
    <Typography
      variant="body2"
      color={quotaAvailable(group) ? "text.primary" : "text.disabled"}
      sx={{ minWidth: 0, overflowWrap: "anywhere" }}
    >
      {uiGroupName(group.name)}
    </Typography>
    <Typography
      variant="caption"
      color="text.secondary"
      sx={{ flexShrink: 0, textAlign: "right" }}
    >
      {quotaMenuBalance(group)}
    </Typography>
  </Box>;
}
function QuotaScale({ value, label }: {
  value: ReturnType<typeof quotaSummary>["group"];
  label: string
}) {
  const dark = useTheme().palette.mode === "dark";
  const progress = value.progress;
  const color = quotaScaleColor(progress, dark);
  return <Stack gap={.75}>
    {progress !== null && <LinearProgress
      variant="determinate"
      value={progress}
      aria-label={label}
      sx={{
        height: 4,
        borderRadius: 1,
        bgcolor: "action.hover",
        "& .MuiLinearProgress-bar": { bgcolor: color }
      }}
    />}
    <Tooltip title={value.tooltip}>
      <Typography
        variant="body2"
        color="text.secondary"
        tabIndex={0}
        sx={{ overflowWrap: "anywhere" }}
      >
        {value.line}
      </Typography>
    </Tooltip>
  </Stack>;
}
export function GroupUsage({ groups, selected, onChange, actions, notice }: {
  groups: AccessGroup[];
  selected: number | "";
  onChange: (id: number) => void;
  actions?: ReactNode;
  notice?: ReactNode;
}) {
  const group = groups.find(g => g.id === selected) || (groups.length === 1 ? groups[0] : null);
  const summary = group ? quotaSummary(group) : null;
  if (!groups.length) return actions || notice ? <Stack
    spacing={1}
    sx={{ mb: 2 }}
  >
    {actions}
    {notice}
  </Stack> : null;
  return <Box
    component="section"
    aria-label={uiText("Квота")}
  >
    {groups.length > 1 && <Typography
      component="p"
      variant="subtitle1"
      fontWeight={700}
      sx={{ mb: 1 }}
    >{uiText("Квота")}</Typography>}
    <Box
      sx={{
        display: "grid",
        gridTemplateColumns: { xs: "minmax(0, 1fr)", md: "minmax(0, 1.3fr) minmax(0, 1fr)" },
        gap: 2,
        alignItems: "start"
      }}
    >
      {groups.length === 1 ? <Typography
        component="div"
        variant="subtitle1"
        fontWeight={700}
        sx={{ py: .75, overflowWrap: "anywhere" }}
      >{uiText("Квота:") + " "}{uiGroupName(groups[0].name)}
        {selected !== groups[0].id && quotaAvailable(groups[0]) && <Button
          color="secondary"
          size="small"
          sx={{ ml: 1 }}
          onClick={() => onChange(groups[0].id)}
        >{uiText("Использовать эту квоту")}</Button>}
      </Typography> : <TextField
        select
        fullWidth
        size="small"
        label={uiText("Группа для расхода отчёта")}
        value={selected}
        InputLabelProps={{ shrink: true }}
        onChange={e => onChange(Number(e.target.value))}
        SelectProps={{
          displayEmpty: true,
          renderValue: value => <span data-quota-field-name="true">
            {quotaFieldName(groups, Number(value) || "")}
          </span>
        }}
      >
        <MenuItem
          value=""
          disabled
          sx={{ display: "none" }}
        >{uiText("Выберите квоту")}</MenuItem>
        {groups.map(
          g => <MenuItem
            key={g.id}
            value={g.id}
            disabled={!quotaAvailable(g)}
            sx={{ whiteSpace: "normal", "&.Mui-disabled": { opacity: 1, color: "text.secondary" } }}
          >
            <QuotaMenuLabel group={g} />
          </MenuItem>
        )}
      </TextField>}
      {actions}
      {summary && <Stack
        spacing={1.25}
        sx={{ gridColumn: { md: 1 }, gridRow: { md: 2 } }}
      >
        <QuotaScale
          value={summary.group}
          label={uiText("Расход группы")}
        />
        {summary.personal && <QuotaScale
          value={summary.personal}
          label={uiText("Расход по личному лимиту")}
        />}
      </Stack>}
      <Box sx={{ minWidth: 0, gridColumn: { md: 2 }, gridRow: { md: 2 } }}>
        {notice}
      </Box>
    </Box>
  </Box>;
}
