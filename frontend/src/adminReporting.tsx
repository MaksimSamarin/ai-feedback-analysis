import { uiText, uiLocale } from "./i18n";
import { UserIdentity, type Profile } from "./userIdentity";
import { Fragment, useEffect, useMemo, useState } from "react";
import {
  Box,
  Button,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
  Menu,
  MenuItem,
  Stack,
  Tab,
  Tabs,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TableSortLabel,
  TextField,
  ToggleButton,
  ToggleButtonGroup,
  Tooltip,
  Typography,
  useTheme
} from "@mui/material";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import DownloadIcon from "@mui/icons-material/Download";
import {
  Bar,
  CartesianGrid,
  ComposedChart,
  Legend,
  Line,
  ReferenceLine,
  ResponsiveContainer,
  Tooltip as ChartTooltip,
  XAxis,
  YAxis
} from "recharts";
import { accessCall, type AccessGroup } from "./access";
import { api } from "./api";
import { Reports } from "./adminReports";
import {
  Empty,
  Failure,
  Kpi,
  Loading,
  Panel,
  UsageBar,
  compact,
  moscowDate,
  quantity
} from "./adminShared";
import { reliableSinceText } from "./dateFormat";
type Metrics = {
  total_tokens: number;
  reports: number;
  processed_rows: number;
  active_users: number;
  error_reports: number;
  error_rows: number;
  error_report_percent: number;
  error_row_percent: number;
  average_tokens: number;
  forecast: number | null;
  forecast_is_estimate?: boolean;
  unknown_row_metrics: number;
  limit_stops: number
};
type GroupMetrics = Metrics & {
  id: number;
  name: string;
  token_limit: number | null;
  member_count: number;
  percent: number | null;
  leaders: string[]
};
type UserMetrics = Profile & Metrics & {
  id: number;
  username: string;
  last_activity: string
};
type Data = {
  summary: Metrics & {
    token_limit: number | null;
    limit_percent: number | null;
    comparison: Record<string, {
      delta: number;
      percent: number | null
    }>
  };
  groups: GroupMetrics[];
  users: UserMetrics[];
  daily: Array<{
    date: string;
    group_id: number;
    group_name: string;
    model?: string;
    total_tokens: number
  }>;
  models?: Array<Metrics & {
    provider: string;
    model: string
  }>;
  modes?: Array<Metrics & {
    mode: string
  }>;
  reliable_since: string | null;
  period: {
    from: string;
    to: string;
    previous_from: string;
    previous_to: string
  }
};
const colors = ["#47658d", "#21806f", "#9074ad", "#b28522", "#508b9a", "#6a7d43"];
function dateOnly(value: string) { return new Intl.DateTimeFormat("sv-SE", { timeZone: "Europe/Moscow" }).format(new Date(value)); }
function addDay(value: string) {
  const d = new Date(value + "T12:00:00Z");
  d.setUTCDate(d.getUTCDate() + 1);
  return d.toISOString().slice(0, 10);
}
export function periodLabel(period: Data["period"]) {
  const start = new Date(period.from), end = new Date(Date.parse(period.to) - 1);
  const day = (d: Date) => new Intl.DateTimeFormat(uiLocale(), { timeZone: "Europe/Moscow", day: "numeric" }).format(d);
  const month = new Intl.DateTimeFormat(
    uiLocale(),
    { timeZone: "Europe/Moscow", month: "long", year: "numeric", day: "numeric" }
  ).format(end).replace(/^\d+\s*/, "").replace(/\s*г\.$/, "");
  const same = dateOnly(period.from).slice(0, 7) === dateOnly(end.toISOString()).slice(0, 7);
  const priorStart = dateOnly(period.previous_from), priorEnd = dateOnly(new Date(Date.parse(period.previous_to) - 1).toISOString());
  const months = [
    uiText("январём"),
    uiText("февралём"),
    uiText("мартом"),
    uiText("апрелем"),
    uiText("маем"),
    uiText("июнем"),
    uiText("июлем"),
    uiText("августом"),
    uiText("сентябрём"),
    uiText("октябрём"),
    uiText("ноябрём"),
    uiText("декабрём")
  ];
  const previous = priorStart.slice(0, 7) === priorEnd.slice(0, 7)
    ? months[Number(priorStart.slice(5, 7)) - 1]
    : `${moscowDate(period.previous_from).slice(0, 10)}–${moscowDate(new Date(Date.parse(period.previous_to) - 1).toISOString()).slice(0, 10)}`;
  const current = same ? `${day(start)}–${day(end)} ${month}`
    : `${moscowDate(period.from).slice(0, 10)}–${moscowDate(end.toISOString()).slice(0, 10)}`;
  return uiText("{0} · сравнение с {1}", current, previous);
}
export function ReportingView({ isAdmin, groups, revision }: {
  isAdmin: boolean;
  groups: AccessGroup[];
  revision: number
}) {
  const theme = useTheme();
  const [preset, setPreset] = useState("month"), [from, setFrom] = useState(""), [to, setTo] = useState("");
  const [group, setGroup] = useState<number | "">(""), [user, setUser] = useState<number | undefined>();
  const [search, setSearch] = useState(""), [view, setView] = useState("groups");
  const [byModel, setByModel] = useState(false), [cumulative, setCumulative] = useState(false);
  const [data, setData] = useState<Data | null>(null), [busy, setBusy] = useState(true), [error, setError] = useState("");
  const [sort, setSort] = useState("total_tokens"), [direction, setDirection] = useState<"asc" | "desc">("desc");
  const [expanded, setExpanded] = useState<number | null>(null), [exportAnchor, setExportAnchor] = useState<HTMLElement | null>(null);
  const [business, setBusiness] = useState(false), [businessMonth, setBusinessMonth] = useState(() => dateOnly(new Date().toISOString()).slice(0, 7));
  const query = new URLSearchParams({ preset });
  if (preset === "custom") {
    query.set("date_from", from);
    if (to) query.set("date_to", addDay(to));
  }
  if (group !== "") query.set("group_id", String(group));
  if (search) query.set("search", search);
  const key = query.toString();
  useEffect(
    () => {
      if (!isAdmin) {
        setView("groups");
        setByModel(false);
      }
    },
    [isAdmin]
  );
  useEffect(
    () => {
      if (preset === "custom" && (!from || !to)) {
        setBusy(false);
        return;
      }
      let active = true;
      setBusy(true);
      const timer = setTimeout(
        () => void accessCall<Data>(`/group-admin/reporting?${key}`)
          .then(
            d => {
              if (active) {
                setData(d);
                setError("");
              }
            }
          ).catch(e => active && setError(e.message))
          .finally(() => active && setBusy(false)),
        200
      );
      return () => {
        active = false;
        clearTimeout(timer);
      };
    },
    [key, revision, isAdmin]
  );
  const chart = useMemo(
    () => {
      const dates = new Map<string, Record<string, string | number>>(), names = new Set<string>();
      (data?.daily || []).forEach(
        item => {
          const name = isAdmin && byModel ? item.model || uiText("Не указана") : item.group_name;
          names.add(name);
          const row = dates.get(item.date) || { date: item.date };
          row[name] = Number(row[name] || 0) + item.total_tokens;
          dates.set(item.date, row);
        }
      );
      if (data && dates.size) {
        const until = Math.min(
          Date.parse(data.period.to),
          Date.parse(dateOnly(new Date().toISOString()) + "T00:00:00+03:00") + 86400000
        );
        for (let day = Date.parse(data.period.from); day < until; day += 86400000) {
          const label = dateOnly(new Date(day).toISOString());
          if (!dates.has(label)) dates.set(label, { date: label });
        }
      }
      const totals: Record<string, number> = {};
      const rows = [...dates].sort(([a], [b]) => a.localeCompare(b)).map(
        ([, row]) => {
          names.forEach(
            name => {
              totals[name] = (totals[name] || 0) + Number(row[name] || 0);
              row[name] = cumulative ? totals[name] : Number(row[name] || 0);
            }
          );
          return row;
        }
      );
      return { rows, names: [...names] };
    },
    [data, byModel, cumulative, isAdmin]
  );
  const root = api.base.endsWith("/api") ? api.base : api.base + "/api";
  function exportUrl(name: string) {
    const p = new URLSearchParams(key);
    p.set("export", name);
    if (name === "reports" && user) p.set("user_id", String(user));
    return `${root}/group-admin/reporting?${p}`;
  }
  const [year, month] = businessMonth.split("-").map(Number);
  const businessEnd = month === 12 ? `${year + 1}-01-01` : `${year}-${String(month + 1).padStart(2, "0")}-01`;
  const bq = new URLSearchParams(
    {
      preset: "custom",
      date_from: `${businessMonth}-01`,
      date_to: businessEnd,
      export: "business"
    }
  );
  if (group !== "") bq.set("group_id", String(group));
  function order(field: string) {
    setDirection(sort === field && direction === "desc" ? "asc" : "desc");
    setSort(field);
  }
  const sorted = <T extends object>(rows: T[]) => [...rows].sort(
    (a, b) => {
      const x = (a as Record<string, unknown>)[sort], y = (b as Record<string, unknown>)[sort];
      const n = typeof x === "string" ? x.localeCompare(String(y || ""), "ru") : Number(x || 0) - Number(y || 0);
      return direction === "asc" ? n : -n;
    }
  );
  const heading = (name: string, field: string, numeric = true) => <TableCell align={numeric ? "right" : "left"}>
    <TableSortLabel
      active={sort === field}
      direction={sort === field ? direction : "desc"}
      onClick={() => order(field)}
    >
      {name}
    </TableSortLabel>
  </TableCell>;
  const cards: Array<[string, keyof Metrics]> = [
    [uiText("Израсходовано токенов"), "total_tokens"],
    [uiText("Оценка до конца периода"), "forecast"],
    [uiText("Отчётов"), "reports"],
    [uiText("Обработано строк"), "processed_rows"],
    [uiText("Активных за период"), "active_users"],
    [uiText("Отчётов с ошибками, %"), "error_report_percent"],
    [uiText("Строк с ошибками, %"), "error_row_percent"],
    [uiText("Токенов на строку"), "average_tokens"]
  ];
  return <>
    <Panel
      title={uiText("Отчётность")}
      actions={<Button
        variant="outlined"
        color="inherit"
        startIcon={<DownloadIcon />}
        onClick={e => setExportAnchor(e.currentTarget)}
      >{uiText("Экспорт ▾")}</Button>}
    >
      <Box className="admin-filter-row">
        <TextField
          select
          label={uiText("Период")}
          value={preset}
          onChange={e => setPreset(e.target.value)}
        >
          {[
            ["month", uiText("Текущий месяц")],
            ["previous", uiText("Прошлый месяц")],
            ["quarter", uiText("Текущий квартал")],
            ["custom", uiText("Выбрать даты")]
          ]
            .map(
              ([v, label]) => <MenuItem
                key={v}
                value={v}
              >
                {label}
              </MenuItem>
            )}
        </TextField>
        <TextField
          select
          label={uiText("Группа")}
          value={group}
          onChange={e => {
            setGroup(e.target.value === "" ? "" : Number(e.target.value));
            setUser(undefined);
            setView("groups");
          }}
        >
          <MenuItem value="">{uiText("Все доступные группы")}</MenuItem>
          {isAdmin && <MenuItem value={0}>{uiText("Без группы")}</MenuItem>}
          {groups.map(
            g => <MenuItem
              key={g.id}
              value={g.id}
            >
              {g.name}
            </MenuItem>
          )}
        </TextField>
        <TextField
          label={uiText("Поиск пользователя")}
          value={search}
          onChange={e => {
            setSearch(e.target.value);
            setUser(undefined);
            setView("users");
          }}
        />
        {preset === "custom" && <>
          <TextField
            label={uiText("С даты")}
            type="date"
            value={from}
            InputLabelProps={{ shrink: true }}
            onChange={e => setFrom(e.target.value)}
          />
          <TextField
            label={uiText("По дату")}
            type="date"
            value={to}
            InputLabelProps={{ shrink: true }}
            onChange={e => setTo(e.target.value)}
          />
        </>}
      </Box>
      {data && <Stack
        direction="row"
        alignItems="center"
        gap={.5}
      >
        <Typography
          variant="body2"
          color="text.secondary"
        >
          {periodLabel(data.period)}
        </Typography>
        <Tooltip
          title={<Box>{reliableSinceText(data.reliable_since)}{" " + uiText("Удалённые отчёты включены. Расход относится к дате запуска. Лимиты — действующие настройки групп. Границы периода считаются по Москве.")}{!!data.summary.unknown_row_metrics && uiText("У {0} старых отчётов детализация ошибок не сохранилась.", data.summary.unknown_row_metrics)}</Box>}
        >
          <IconButton
            size="small"
            aria-label={uiText("О периоде и учёте")}
          >
            <InfoOutlinedIcon fontSize="small" />
          </IconButton>
        </Tooltip>
      </Stack>}
      <Failure error={error} />
    </Panel>
    {busy && !data ? <Loading /> : data && <>
      <Box className="admin-kpis">
        {cards.map(
          ([label, field]) => {
            const delta = data.summary.comparison[field];
            const bad = ["total_tokens", "error_reports"].includes(field);
            const positive = delta && (bad ? delta.delta < 0 : delta.delta > 0);
            return <Kpi
              key={field}
              label={label}
              value={Number.isFinite(data.summary[field]) ? Number(data.summary[field]) : null}
              footer={delta?.percent != null ? <Typography
                variant="caption"
                color={positive ? "success.main" : "error.main"}
              >
                {delta.delta >= 0 ? "↑" : "↓"} {Math.abs(delta.percent).toFixed(1)}{" " + uiText("% к прошлому периоду")}</Typography> : field === "forecast" && data.summary.forecast !== null ? <Typography
                variant="caption"
                color="text.secondary"
              >{uiText("Оценка, не гарантия")}</Typography> : undefined}
            />;
          }
        )}
      </Box>
      <Panel title={uiText("Расход по дням")}>
        <Stack
          direction="row"
          gap={2}
          flexWrap="wrap"
          sx={{ mb: 2 }}
        >
          <ToggleButtonGroup
            size="small"
            exclusive
            value={cumulative ? "sum" : "day"}
            onChange={(_, v) => v && setCumulative(v === "sum")}
            aria-label={uiText("Вид динамики")}
          >
            <ToggleButton value="day">{uiText("По дням")}</ToggleButton>
            <ToggleButton value="sum">{uiText("Накопительно")}</ToggleButton>
          </ToggleButtonGroup>
          {isAdmin && <ToggleButtonGroup
            size="small"
            exclusive
            value={byModel ? "models" : "groups"}
            onChange={(_, v) => v && setByModel(v === "models")}
            aria-label={uiText("Разбивка динамики")}
          >
            <ToggleButton value="groups">{uiText("По группам")}</ToggleButton>
            <ToggleButton value="models">{uiText("По моделям")}</ToggleButton>
          </ToggleButtonGroup>}
        </Stack>
        {chart.rows.length ? <Box sx={{ width: "100%", height: 300 }}>
          <ResponsiveContainer>
            <ComposedChart
              data={chart.rows}
              margin={{ top: 20, right: 24, left: 0, bottom: 8 }}
            >
              <CartesianGrid
                stroke={theme.palette.divider}
                strokeDasharray="3 3"
                vertical={false}
              />
              <XAxis
                dataKey="date"
                tickFormatter={v => String(v).slice(5).split("-").reverse().join(".")}
                tick={{ fill: theme.palette.text.secondary, fontSize: 12 }}
              />
              <YAxis
                width={64}
                tickFormatter={v => compact(Number(v))}
                tick={{ fill: theme.palette.text.secondary, fontSize: 12 }}
              />
              <ChartTooltip
                formatter={v => quantity(Number(v))}
                contentStyle={{ background: theme.palette.background.paper, borderColor: theme.palette.divider }}
              />
              <Legend
                wrapperStyle={{ fontSize: 13, paddingTop: 12 }}
                formatter={value => <span style={{ color: theme.palette.text.primary }}>
                  {value}
                </span>}
              />
              {chart.names.map(
                (name, i) => cumulative
                  ? <Line
                    key={name}
                    dataKey={name}
                    stroke={colors[i % colors.length]}
                    strokeWidth={2}
                    dot={false}
                    isAnimationActive={false}
                  />
                  : <Bar
                    key={name}
                    dataKey={name}
                    stackId="tokens"
                    fill={colors[i % colors.length]}
                    isAnimationActive={false}
                  />
              )}
              {data.summary.token_limit !== null && <ReferenceLine
                y={data.summary.token_limit}
                label={{ value: uiText("Лимит группы"), fill: theme.palette.text.secondary }}
                stroke={theme.palette.warning.main}
                strokeDasharray="5 4"
                ifOverflow="extendDomain"
              />}
            </ComposedChart>
          </ResponsiveContainer>
        </Box> : <Empty text={uiText("За этот период расхода нет. Выберите другой период или группу.")} />}
      </Panel>
      <Panel>
        <Tabs
          value={view}
          variant="scrollable"
          scrollButtons="auto"
          onChange={(_, v) => {
            setView(v);
            setUser(undefined);
          }}
        >
          <Tab
            value="groups"
            label={uiText("Группы")}
          />
          <Tab
            value="users"
            label={uiText("Пользователи")}
          />
          {isAdmin && <Tab
            value="models"
            label={uiText("Модели")}
          />}
          {isAdmin && <Tab
            value="modes"
            label={uiText("Режимы")}
          />}
          {view === "reports" && <Tab
            value="reports"
            label={uiText("Отчёты пользователя")}
          />}
        </Tabs>
        {view === "groups" && <>
          <TextField
            select
            size="small"
            label={uiText("Сортировать по")}
            value={sort}
            sx={{ display: { xs: "inline-flex", md: "none" }, mt: 2, mb: 1, minWidth: 200 }}
            onChange={e => setSort(e.target.value)}
          >
            {[
              ["name", uiText("Название")],
              ["token_limit", uiText("Лимит")],
              ["total_tokens", uiText("Расход")],
              ["percent", uiText("Процент")],
              ["forecast", uiText("Оценка")],
              ["member_count", uiText("Участники")],
              ["active_users", uiText("Активные")],
              ["reports", uiText("Отчёты")],
              ["error_reports", uiText("Ошибки")],
              ["limit_stops", uiText("Остановки")]
            ]
              .map(
                ([v, label]) => <MenuItem
                  key={v}
                  value={v}
                >
                  {label}
                </MenuItem>
              )}
          </TextField>
          <TableContainer className="reporting-groups-table">
            <Table
              size="small"
              className="admin-table"
            >
              <TableHead>
                <TableRow>
                  {heading(uiText("Группа"), "name", false)}
                  {heading(uiText("Расход / лимит"), "total_tokens", false)}
                  {heading(uiText("Оценка"), "forecast")}
                  {heading(uiText("Отчёты"), "reports")}
                  <TableCell align="right">
                    <Tooltip title={uiText("Активных за период, включая бывших участников")}>
                      <span>{uiText("Активные")}</span>
                    </Tooltip>
                  </TableCell>
                  <TableCell />
                </TableRow>
              </TableHead>
              <TableBody>
                {sorted(data.groups).map(
                  g => <Fragment key={g.id}>
                    <TableRow hover>
                      <TableCell className="reporting-group-name">
                        <Tooltip title={g.name}>
                          <Button
                            color="inherit"
                            className="admin-text-link admin-ellipsis"
                            onClick={() => {
                              setGroup(g.id);
                              setView("users");
                            }}
                          >
                            {g.name}
                          </Button>
                        </Tooltip>
                      </TableCell>
                      <TableCell>
                        <UsageBar
                          used={g.total_tokens}
                          limit={g.token_limit}
                          dense
                        />
                      </TableCell>
                      <TableCell align="right">
                        {g.forecast === null ? uiText("Мало данных") : quantity(g.forecast)}
                      </TableCell>
                      <TableCell align="right">
                        {quantity(g.reports)}
                      </TableCell>
                      <TableCell align="right">
                        {quantity(g.active_users)}
                      </TableCell>
                      <TableCell>
                        <IconButton
                          aria-label={uiText("Подробнее о {0}", g.name)}
                          aria-expanded={expanded === g.id}
                          onClick={() => setExpanded(expanded === g.id ? null : g.id)}
                        >
                          {expanded === g.id ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                        </IconButton>
                      </TableCell>
                    </TableRow>
                    <TableRow>
                      <TableCell
                        colSpan={6}
                        sx={{ p: 0, border: 0 }}
                      >
                        <Collapse
                          in={expanded === g.id}
                          unmountOnExit
                        >
                          <Box className="admin-row-detail">
                            <Typography variant="body2">{uiText("Участников сейчас:") + " "}{quantity(g.member_count)}{uiText(". Активных за период (включая бывших участников):") + " "}{quantity(g.active_users)}.</Typography>
                            <Typography variant="body2">{uiText("Отчётов с ошибками:") + " "}{quantity(g.error_reports)}{" " + uiText("· строк с ошибками:") + " "}{quantity(g.error_rows)}{" " + uiText("· остановок по лимиту:") + " "}{quantity(g.limit_stops)}</Typography>
                            <Typography variant="body2">{uiText("Руководители:") + " "}{g.leaders.join(", ") || uiText("Не назначены")}</Typography>
                          </Box>
                        </Collapse>
                      </TableCell>
                    </TableRow>
                  </Fragment>
                )}
              </TableBody>
            </Table>
          </TableContainer>
        </>}
        {view === "users" && <TableContainer
          className="admin-stat-table"
          sx={{ mt: 2 }}
        >
          <Table
            size="small"
            className="admin-table"
          >
            <TableHead>
              <TableRow>
                {heading(uiText("Пользователь"), "username", false)}
                {heading(uiText("Токены"), "total_tokens")}
                {heading(uiText("Отчёты"), "reports")}
                {heading(uiText("Строки"), "processed_rows")}
                {heading(uiText("Ошибки"), "error_rows")}
                {heading(uiText("Активность"), "last_activity", false)}
              </TableRow>
            </TableHead>
            <TableBody>
              {sorted(data.users).map(
                u => <TableRow
                  key={u.id}
                  hover
                >
                  <TableCell>
                    <Button
                      color="inherit"
                      className="admin-text-link"
                      onClick={() => {
                        setUser(u.id);
                        setView("reports");
                      }}
                    >
                      <UserIdentity user={u} />
                    </Button>
                  </TableCell>
                  {[u.total_tokens, u.reports, u.processed_rows, u.error_rows].map(
                    (v, i) => <TableCell
                      data-label={[uiText("Токены"), uiText("Отчёты"), uiText("Строки"), uiText("Ошибки")][i]}
                      align="right"
                      key={i}
                    >
                      {quantity(v)}
                    </TableCell>
                  )}
                  <TableCell>
                    {moscowDate(u.last_activity)}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>}
        {isAdmin && (view === "models" || view === "modes") && <TableContainer
          className="admin-stat-table"
          sx={{ mt: 2 }}
        >
          <Table
            size="small"
            className="admin-table"
          >
            <TableHead>
              <TableRow>
                <TableCell>
                  {view === "models" ? uiText("Провайдер / модель") : uiText("Режим")}
                </TableCell>
                {heading(uiText("Токены"), "total_tokens")}
                {heading(uiText("Отчёты"), "reports")}
                {heading(uiText("Строки"), "processed_rows")}
                {heading(uiText("Ошибки"), "error_rows")}
              </TableRow>
            </TableHead>
            <TableBody>
              {sorted<Metrics & {
                provider?: string;
                model?: string;
                mode?: string
              }>((view === "models" ? data.models : data.modes) || []).map(
                (r, i) => <TableRow key={i}>
                  <TableCell>
                    {view === "models" ? `${r.provider || uiText("Не указан")} / ${r.model || uiText("Не указана")}` : r.mode === "grouped" ? uiText("Группировка") : uiText("Построчно")}
                  </TableCell>
                  {[r.total_tokens, r.reports, r.processed_rows, r.error_rows].map(
                    (v, j) => <TableCell
                      data-label={[uiText("Токены"), uiText("Отчёты"), uiText("Строки"), uiText("Ошибки")][j]}
                      align="right"
                      key={j}
                    >
                      {quantity(v)}
                    </TableCell>
                  )}
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>}
        {view === "reports" && <Reports
          isAdmin={isAdmin}
          groupId={group === "" ? undefined : group}
          userId={user}
          period={data.period}
          revision={revision}
        />}
        {view !== "reports" && !(view === "groups" ? data.groups : view === "users" ? data.users : view === "models" ? data.models : data.modes)?.length && <Empty text={uiText("По выбранным условиям данных нет. Измените период или фильтры.")} />}
      </Panel>
    </>}
    <Menu
      anchorEl={exportAnchor}
      open={!!exportAnchor}
      onClose={() => setExportAnchor(null)}
    >
      {[
        [view, uiText("Текущее представление")],
        ["summary", uiText("Сводка")],
        ["daily", uiText("Динамика по дням")]
      ].map(
        ([v, label]) => <MenuItem
          key={v}
          component="a"
          href={exportUrl(v)}
          onClick={() => setExportAnchor(null)}
        >{label} · XLSX</MenuItem>
      )}
      <MenuItem onClick={() => {
        setExportAnchor(null);
        setBusiness(true);
      }}>{uiText("Отчёт для бизнеса за месяц…")}</MenuItem>
    </Menu>
    <Dialog
      open={business}
      onClose={() => setBusiness(false)}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle>{uiText("Отчёт для бизнеса за месяц")}</DialogTitle>
      <DialogContent>
        <Stack
          gap={2}
          sx={{ pt: 1 }}
        >
          <TextField
            type="month"
            label={uiText("Месяц по Москве")}
            value={businessMonth}
            InputLabelProps={{ shrink: true }}
            onChange={e => setBusinessMonth(e.target.value)}
          />
          <Typography>{uiText("Все участники выбранных групп. Удалённые отчёты включены.")}</Typography>
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={() => setBusiness(false)}>{uiText("Закрыть")}</Button>
        <Button
          variant="contained"
          disabled={!/^\d{4}-\d{2}$/.test(businessMonth)}
          href={`${root}/group-admin/reporting?${bq}`}
        >{uiText("Скачать XLSX")}</Button>
      </DialogActions>
    </Dialog>
  </>;
}
