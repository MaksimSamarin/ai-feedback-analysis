import { uiText, uiLocale } from "./i18n";
import { useEffect, useState, type ReactNode } from "react";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Chip,
  Skeleton,
  Stack,
  TablePagination,
  Tooltip,
  Typography,
  LinearProgress
} from "@mui/material";
export const quantity = (n: number | null | undefined) => (n ?? 0).toLocaleString(uiLocale());
export const compact = (n: number) => new Intl.NumberFormat(uiLocale(), {
  notation: "compact", maximumFractionDigits: 1,
}).format(n);
export function UsageBar({ used, limit, dense = false, summary = false }: {
  used: number;
  limit: number | null;
  dense?: boolean;
  summary?: boolean;
}) {
  const pct = limit === null ? null : limit === 0 ? 100 : used / limit * 100;
  const percent = pct?.toLocaleString(uiLocale(), { maximumFractionDigits: 1 });
  return <Box className={summary ? "usage-list" : dense ? "usage-mini" : "usage-block"}>
    {summary ? <Tooltip
      title={uiText("{0}{1} токенов{2}", quantity(used), limit !== null ? uiText("из {0}", quantity(limit)) : "", pct !== null ? ` · ${percent} %` : "")}
    >
      <Box className="usage-list-values">
        <Typography
          variant="body2"
          className="admin-ellipsis"
        >
          {compact(used)}
          {limit !== null ? uiText("из {0}", compact(limit)) : " " + uiText("токенов")}
        </Typography>
        {pct !== null && <Typography
          variant="caption"
          color="text.secondary"
        >{percent} %</Typography>}
      </Box>
    </Tooltip> : <Typography
      variant={dense ? "body2" : "h6"}
      color="text.primary"
      sx={{ fontVariantNumeric: "tabular-nums" }}
    >
      {quantity(used)}
      {limit !== null && uiText("из {0}", quantity(limit))}
      {" "}
      {!dense && uiText("токенов")}
      {pct !== null && <Typography
        component="span"
        variant="body2"
        color="text.secondary"
      > · {pct.toLocaleString(uiLocale(), { maximumFractionDigits: 1 })} %</Typography>}
    </Typography>}
    {pct === null ? <Chip
      size="small"
      label={uiText("Без лимита")}
      variant="outlined"
      sx={{ mt: .5 }}
    />
      : <LinearProgress
        variant="determinate"
        value={Math.min(100, pct)}
        color={pct >= 100 ? "error" : pct >= 80 ? "warning" : "success"}
        sx={{ mt: 1, height: dense ? 4 : 6, borderRadius: 1, bgcolor: "action.hover" }}
      />}
  </Box>;
}
export function moscowDate(value: string | null | undefined, seconds = false) {
  if (!value) return "—";
  return new Intl.DateTimeFormat(
    uiLocale(),
    {
      timeZone: "Europe/Moscow",
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
      ...(seconds ? { second: "2-digit" } : {}),
    }
  ).format(new Date(value)).replace(",", "") + " " + uiText("МСК");
}
export function localMoscow(value?: string | null) {
  if (!value) return "";
  const parts = new Intl.DateTimeFormat(
    "sv-SE",
    {
      timeZone: "Europe/Moscow",
      year: "numeric",
      month: "2-digit",
      day: "2-digit",
      hour: "2-digit",
      minute: "2-digit",
      hourCycle: "h23",
    }
  ).format(new Date(value));
  return parts.replace(" ", "T");
}
export const utcMoscow = (value: string) => value ? new Date(value + ":00+03:00").toISOString() : "";
export function navigate(hash: string, replace = false) {
  window.history[replace ? "replaceState" : "pushState"]({}, "", window.location.pathname + window.location.search + hash);
  window.dispatchEvent(new PopStateEvent("popstate"));
}
export function useLocationHash() {
  const [hash, setHash] = useState(window.location.hash);
  useEffect(
    () => {
      const changed = () => setHash(window.location.hash);
      window.addEventListener("popstate", changed);
      window.addEventListener("hashchange", changed);
      return () => {
        window.removeEventListener("popstate", changed);
        window.removeEventListener("hashchange", changed);
      };
    },
    []
  );
  return hash;
}
export function Panel({ title, actions, children }: {
  title?: ReactNode;
  actions?: ReactNode;
  children: ReactNode
}) {
  return <Card className="admin-panel">
    <CardContent>
      {(title || actions) && <Stack
        direction="row"
        gap={2}
        justifyContent="space-between"
        alignItems="center"
        flexWrap="wrap"
        sx={{ mb: 2 }}
      >
        <Typography variant="h6">
          {title}
        </Typography>
        {actions}
      </Stack>}
      {children}
    </CardContent>
  </Card>;
}
export function Loading() {
  return <Box
    aria-label={uiText("Загрузка данных")}
    aria-busy="true"
    className="admin-skeleton"
  >
    <Skeleton
      height={32}
      width="40%"
    />
    {[1, 2, 3, 4].map(n => <Skeleton
      key={n}
      height={48}
    />)}
  </Box>;
}
export function Empty({ text, action }: {
  text: string;
  action?: ReactNode
}) {
  return <Stack
    alignItems="center"
    gap={2}
    className="admin-empty"
  >
    <Typography color="text.secondary">
      {text}
    </Typography>
    {action}
  </Stack>;
}
export function Failure({ error }: {
  error: string
}) {
  return error ? <Alert severity="error">
    {error}
  </Alert> : null;
}
export function Pager({ total, page, size, setPage, setSize }: {
  total: number;
  page: number;
  size: number;
  setPage: (n: number) => void;
  setSize: (n: number) => void;
}) {
  return <TablePagination
    component="div"
    count={total}
    page={page}
    rowsPerPage={size}
    onPageChange={(_, p) => setPage(p)}
    onRowsPerPageChange={e => {
      setSize(Number(e.target.value));
      setPage(0);
    }}
    rowsPerPageOptions={[10, 25, 50]}
    labelRowsPerPage={uiText("На странице")}
    labelDisplayedRows={({ from, to, count }) => uiText("{0}–{1} из {2}", from, to, quantity(count))}
  />;
}
export function Kpi({ label, value, footer, onClick }: {
  label: string;
  value: number | null;
  footer?: ReactNode;
  onClick?: () => void;
}) {
  const body = <>
    <Typography
      variant="body2"
      color="text.secondary"
    >
      {label}
    </Typography>
    <Tooltip
      title={value === null ? uiText("Для оценки нужно не меньше трёх полных дней с данными") : quantity(value)}
    >
      <Typography className="admin-kpi-value">
        {value === null ? uiText("Мало данных") : compact(value)}
      </Typography>
    </Tooltip>
    {footer && <Box className="admin-kpi-footer">
      {footer}
    </Box>}
  </>;
  return <Card className="admin-panel admin-kpi">
    {onClick
      ? <Button
        className="admin-kpi-button"
        onClick={onClick}
      >
        {body}
      </Button> : <CardContent>
        {body}
      </CardContent>}
  </Card>;
}
const statuses: Record<string, string> = {
  queued: uiText("В очереди"),
  running: uiText("В работе"),
  paused: uiText("На паузе"),
  completed: uiText("Готов"),
  failed: uiText("Ошибка"),
  canceled: uiText("Отменён")
};
export type Outcome = {
  status: string;
  status_label?: string;
  all_rows_failed?: boolean;
  has_errors?: boolean;
  failed_rows?: number | null;
  processed_rows?: number;
  retry_hint?: string | null
};
export function StateChip({ row }: {
  row: Outcome
}) {
  const all = row.all_rows_failed || (row.status === "completed" && !!row.processed_rows &&
    (row.failed_rows || 0) >= row.processed_rows);
  const errors = row.has_errors || !!row.failed_rows;
  const label = row.status_label || (all ? uiText("Ошибка: {0}", quantity(row.failed_rows)) : row.status === "completed" && errors
    ? uiText("Готов, ошибок: {0}", quantity(row.failed_rows)) : statuses[row.status] || row.status);
  return <Chip
    size="small"
    label={label}
    title={row.retry_hint || undefined}
    color={all || row.status === "failed" ? "error"
      : errors || row.status === "paused" ? "warning" : row.status === "completed" ? "success" : "default"}
    variant="outlined"
  />;
}
