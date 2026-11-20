import { uiText, uiLocale } from "./i18n";
import { InterfaceSettings } from "./interfaceSettings";
import { useEffect, useState, type ReactNode } from "react";
import {
  Badge,
  Box,
  Button,
  IconButton,
  Stack,
  Switch,
  Tooltip,
  Typography
} from "@mui/material";
import ArrowBackIcon from "@mui/icons-material/ArrowBack";
import RefreshIcon from "@mui/icons-material/Refresh";
import { Bar, BarChart, ResponsiveContainer, Tooltip as ChartTooltip, XAxis } from "recharts";
import { api } from "./api";
import { accessCall, type AccessGroup } from "./access";
import type { AdminStats } from "./types";
import {
  Empty,
  Failure,
  Kpi,
  Loading,
  Panel,
  StateChip,
  moscowDate,
  navigate,
  quantity
} from "./adminShared";
import { Groups } from "./adminGroups";
import { Requests, Users, type RequestRow } from "./adminPeople";
import { Reports, type ReportMetadata } from "./adminReports";
import { EventSearch, AuditSearch } from "./adminEvents";
import { ModelSetupNotice } from "./modelSetup";
import { Models } from "./adminModels";
import { ReportingView } from "./adminReporting";
import "./admin.css";
type Daily = {
  date: string;
  total_tokens: number
};
type Summary = {
  summary: {
    total_tokens: number
  };
  daily: Daily[]
};
export function AdminScreen({ isAdmin, userId, hash, onChanged, menuButton, accountMenu, logoSrc }: {
  isAdmin: boolean;
  userId: number;
  hash: string;
  onChanged: () => void;
  menuButton: ReactNode;
  accountMenu: ReactNode;
  logoSrc: string;
}) {
  const section = hash.split("?")[0].split("/")[2] || (isAdmin ? "overview" : "groups");
  useEffect(
    () => {
      window.scrollTo(0, 0);
      document.querySelector('.admin-nav [aria-current="page"]')?.scrollIntoView({ block: "nearest", inline: "nearest" });
    },
    [section]
  );
  const [groups, setGroups] = useState<AccessGroup[]>([]), [requests, setRequests] = useState<RequestRow[]>([]);
  const [modelsEmpty, setModelsEmpty] = useState(false);
  const [stats, setStats] = useState<AdminStats | null>(null), [tokens, setTokens] = useState(0);
  const [daily, setDaily] = useState<Daily[]>([]), [failures, setFailures] = useState<ReportMetadata[]>([]);
  const [errorCount, setErrorCount] = useState(0);
  const [busy, setBusy] = useState(true), [error, setError] = useState(""), [revision, setRevision] = useState(0);
  const [auto, setAuto] = useState(false), [updated, setUpdated] = useState<string | null>(null);
  const sections = isAdmin ? [
    ["overview", uiText("Обзор")],
    ["groups", uiText("Группы")],
    ["users", uiText("Пользователи")],
    ["models", uiText("Модели")],
    ["requests", uiText("Заявки")],
    ["reporting", uiText("Отчётность")],
    ["reports", uiText("Отчёты пользователей")],
    ["logs", uiText("Логи")],
    ["audit", uiText("Журнал изменений")],
    ["settings", uiText("Настройки")]
  ]
    : [
      ["groups", uiText("Группы")],
      ["requests", uiText("Заявки")],
      ["reporting", uiText("Отчётность")],
      ["audit", uiText("Журнал событий")]
    ];
  useEffect(
    () => {
      if (!sections.some(([key]) => key === section)) navigate(`#/admin/${isAdmin ? "overview" : "groups"}`, true);
    },
    [section, isAdmin]
  );
  async function refresh() {
    setBusy(true);
    setError("");
    try {
      const [g, r] = await Promise.all(
        [
          accessCall<AccessGroup[]>("/group-admin/groups"),
          accessCall<RequestRow[]>("/group-admin/requests")
        ]
      );
      setGroups(g);
      setRequests(r);
      if (isAdmin) {
        setModelsEmpty((await api.getProviders()).length === 0);
        const end = new Date();
        end.setUTCDate(end.getUTCDate() + 1);
        const start = new Date();
        start.setUTCDate(start.getUTCDate() - 29);
        const p = new URLSearchParams({ preset: "custom", date_from: start.toISOString(), date_to: end.toISOString() });
        const [state, month, trend, errors] = await Promise.all(
          [
            api.adminStats(),
            accessCall<Summary>("/group-admin/reporting"),
            accessCall<Summary>(`/group-admin/reporting?${p}`),
            accessCall<{
              rows: ReportMetadata[];
              total: number
            }>(
              `/group-admin/reports?status=errors&include_test=false&limit=5&date_from=${encodeURIComponent(new Date(Date.now() - 86400000).toISOString())}`
            )
          ]
        );
        setStats(state);
        setTokens(month.summary.total_tokens);
        setFailures(errors.rows);
        setErrorCount(errors.total);
        const days = new Map<string, number>();
        for (let ago = 29; ago >= 0; ago--) {
          const day = new Date(Date.now() - ago * 86400000);
          const key = new Intl.DateTimeFormat("sv-SE", { timeZone: "Europe/Moscow" }).format(day);
          days.set(key, 0);
        }
        trend.daily.forEach(
          d => { if (days.has(d.date)) days.set(d.date, (days.get(d.date) || 0) + d.total_tokens); }
        );
        setDaily(
          [...days].sort(([a], [b]) => a.localeCompare(b)).map(([date, total_tokens]) => ({ date, total_tokens }))
        );
      }
      setUpdated(new Date().toISOString());
    } catch (e) { setError((e as Error).message); } finally { setBusy(false); }
  }
  useEffect(() => { void refresh(); }, [revision, isAdmin]);
  useEffect(
    () => {
      if (!auto) return;
      const t = setInterval(() => setRevision(v => v + 1), 10000);
      return () => clearInterval(t);
    },
    [auto]
  );
  function changed() {
    setRevision(v => v + 1);
    onChanged();
  }
  const pending = requests.filter(r => r.status === "pending").length;
  const near = groups.filter(
    g => g.is_active && g.token_limit !== null && (g.token_limit === 0 || g.used / g.token_limit >= .8)
  );
  const monthName = new Intl.DateTimeFormat(uiLocale(), { timeZone: "Europe/Moscow", month: "long" }).format(new Date());
  const kpis = [
    [uiText("Ожидают решения"), pending, "requests"],
    [uiText("В работе"), stats?.running || 0, "reports?status=running"],
    [uiText("В очереди"), stats?.queued || 0, "reports?status=queued"],
    [uiText("На паузе"), stats?.paused || 0, "reports?status=paused"],
    [uiText("С ошибками за 24 ч"), errorCount, "reports?status=errors"],
    [uiText("Токенов за {0}", monthName), tokens, "reporting"],
    [uiText("Группы у границы лимита"), near.length, "groups"],
    [uiText("Активные группы"), groups.filter(g => g.is_active).length, "groups"]
  ] as const;
  return <Box className="page admin-page">
    <Box className="admin-container">
      <Box
        component="header"
        className="admin-header"
      >
        <img
          className="admin-logo"
          src={logoSrc}
          alt="Analyzer"
        />
        <Tooltip title={uiText("К отчётам")}>
          <Button
            className="admin-back"
            color="inherit"
            onClick={() => navigate("")}
            aria-label={uiText("К отчётам")}
          >
            <ArrowBackIcon fontSize="small" />
            <span>{uiText("К отчётам")}</span>
          </Button>
        </Tooltip>
        <Typography
          component="h1"
          className="admin-title"
        >
          {isAdmin ? uiText("Администрирование") : uiText("Управление группами")}
        </Typography>
        <Box className="admin-header-controls">
          <Tooltip title={updated ? uiText("Обновлено {0}", moscowDate(updated, true)) : uiText("Обновить данные")}>
            <span>
              <IconButton
                aria-label={uiText("Обновить")}
                disabled={busy}
                onClick={changed}
              >
                <RefreshIcon fontSize="small" />
              </IconButton>
            </span>
          </Tooltip>
          <Tooltip title={uiText("Автоматически обновлять данные")}>
            <Switch
              size="small"
              checked={auto}
              onChange={e => setAuto(e.target.checked)}
              inputProps={{ "aria-label": uiText("Автообновление") }}
            />
          </Tooltip>
          {menuButton}
          {accountMenu}
        </Box>
      </Box>
      <Box className="admin-layout">
        <Box
          component="nav"
          aria-label={uiText("Разделы управления")}
          className="admin-nav"
        >
          {sections.map(
            ([key, title]) => <Button
              key={key}
              color="inherit"
              disableRipple
              className={section === key ? "selected" : ""}
              aria-current={section === key ? "page" : undefined}
              onClick={() => navigate(`#/admin/${key}`, true)}
            >
              {title}
              {key === "requests" && pending > 0 && <Badge
                badgeContent={pending}
                color="primary"
                className="admin-nav-badge"
              />}
            </Button>
          )}
        </Box>
        <Box
          component="main"
          className="admin-content"
          data-ready={!busy}
          data-section={section}
        >
          <Failure error={error} />
          {isAdmin && section === "overview" && modelsEmpty && <ModelSetupNotice isAdmin />}
          {section === "overview" && isAdmin && <>
            <Typography variant="h5">{uiText("Обзор")}</Typography>
            {busy && !updated ? <Loading /> : <Box className="admin-kpis">
              {kpis.map(
                ([label, value, route]) =>
                  <Kpi
                    key={label}
                    label={label}
                    value={value}
                    onClick={() => navigate(`#/admin/${route}`, true)}
                  />
              )}
            </Box>}
            <Box className="admin-overview-grid">
              <Panel
                title={uiText("Ждут решения")}
                actions={<Button
                  color="inherit"
                  onClick={() => navigate("#/admin/requests", true)}
                >{uiText("Все заявки")}</Button>}
              >
                <Requests
                  rows={requests}
                  groups={groups}
                  changed={changed}
                  compact
                />
              </Panel>
              <Panel title={uiText("Требуют внимания")}>
                <Typography variant="subtitle2">{uiText("Группы у границы лимита")}</Typography>
                {near.length ? near.slice(0, 4).map(
                  g => <Button
                    key={g.id}
                    color="inherit"
                    className="admin-list-link"
                    onClick={() => navigate("#/admin/groups", true)}
                  >{g.name} · {quantity(g.used)} / {quantity(g.token_limit)}</Button>
                )
                  : <Typography
                    variant="body2"
                    color="text.secondary"
                    sx={{ py: 1 }}
                  >{uiText("Все группы в пределах лимитов.")}</Typography>}
                <Typography
                  variant="subtitle2"
                  sx={{ mt: 2 }}
                >{uiText("Ошибки за 24 часа")}</Typography>
                {failures.map(
                  r => <Button
                    key={r.id}
                    color="inherit"
                    className="admin-list-link"
                    onClick={() => navigate("#/admin/reports?status=errors", true)}
                  >
                    <span>
                      {r.name || uiText("Отчёт без названия")}
                    </span>
                    <StateChip row={r} />
                  </Button>
                )}
                {!failures.length && <Typography
                  variant="body2"
                  color="text.secondary"
                  sx={{ py: 1 }}
                >{uiText("Ошибок за последние сутки нет.")}</Typography>}
              </Panel>
            </Box>
            <Panel
              title={uiText("Расход за последние 30 дней")}
              actions={<Button
                color="inherit"
                onClick={() => navigate("#/admin/reporting", true)}
              >{uiText("Подробнее")}</Button>}
            >
              {daily.length ? <Box sx={{ height: 170, width: "100%" }}>
                <ResponsiveContainer>
                  <BarChart data={daily}>
                    <XAxis
                      dataKey="date"
                      tickFormatter={v => String(v).slice(5).split("-").reverse().join(".")}
                      tick={{ fill: "var(--admin-secondary)", fontSize: 12 }}
                    />
                    <ChartTooltip formatter={v => quantity(Number(v))} />
                    <Bar
                      dataKey="total_tokens"
                      name="Токены"
                      fill="var(--admin-chart)"
                      radius={[3, 3, 0, 0]}
                      isAnimationActive={false}
                    />
                  </BarChart>
                </ResponsiveContainer>
              </Box> : <Empty text={uiText("Расхода пока нет. Здесь появится динамика после обработки отчётов.")} />}
            </Panel>
            <Stack
              direction="row"
              gap={1}
              alignItems="center"
              flexWrap="wrap"
              className="admin-service-state"
            >
              <Box className={error ? "state-dot warning" : "state-dot"} />
              <Typography variant="caption">{uiText("Состояние сервиса:") + " "}{error ? uiText("требует проверки") : uiText("доступен")}{" " + uiText("· очередь") + " "}{quantity(stats?.queue_depth)}{" " + uiText("· задач в работе") + " "}{quantity(stats?.running)}{" " + uiText("· воркеров") + " "}{quantity(stats?.analysis_workers)} + {quantity(stats?.inspect_workers)}{" " + uiText("проверки файлов")}</Typography>
            </Stack>
          </>}
          {isAdmin && section === "models" && <Models revision={revision} />}
        {section === "groups" && (busy && !updated ? <Loading /> : <Groups
            groups={groups}
            isAdmin={isAdmin}
            userId={userId}
            changed={changed}
          />)}
          {section === "users" && isAdmin && <Users
            groups={groups}
            revision={revision}
            changed={changed}
          />}
          {section === "requests" && <Requests
            rows={requests}
            groups={groups}
            changed={changed}
          />}
          {section === "reports" && isAdmin && <Reports
            isAdmin
            revision={revision}
            hash={hash}
            groups={groups}
          />}
          {section === "reporting" && <ReportingView
            groups={groups}
            revision={revision}
            isAdmin={isAdmin}
          />}
          {section === "logs" && isAdmin && <EventSearch
            hash={hash}
            revision={revision}
            isAdmin
          />}
          {section === "settings" && isAdmin && <InterfaceSettings />}
          {section === "audit" && <AuditSearch
            hash={hash}
            revision={revision}
            isAdmin={isAdmin}
          />}
        </Box>
      </Box>
    </Box>
  </Box>;
}
