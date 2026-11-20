import { uiText } from "./i18n";
import { AdminFilters } from "./adminFilters";
import { Fragment, useEffect, useRef, useState } from "react";
import {
  Autocomplete,
  Box,
  Button,
  Collapse,
  FormControlLabel,
  IconButton,
  MenuItem,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
  useMediaQuery
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import ContentCopyIcon from "@mui/icons-material/ContentCopy";
import FilterListIcon from "@mui/icons-material/FilterList";
import { accessCall } from "./access";
import { api } from "./api";
import {
  Empty,
  Failure,
  Loading,
  Panel,
  localMoscow,
  moscowDate,
  navigate,
  quantity,
  utcMoscow
} from "./adminShared";
type Page<T> = {
  rows: T[];
  total: number;
  has_more: boolean
};
type Event = {
  id: number;
  event_at: string;
  level: string;
  service?: string;
  username: string | null;
  report_id: string | null;
  job_id: string | null;
  request_id?: string;
  event_type: string;
  message: string;
  traceback?: string
};
const defaultLevels = ["INFO", "WARNING", "ERROR", "CRITICAL"];
function Highlight({ text, match }: {
  text: string;
  match: string
}) {
  const at = match ? text.toLowerCase().indexOf(match.toLowerCase()) : -1;
  return at < 0 ? <>
    {text}
  </> : <>
    {text.slice(0, at)}
    <mark>
      {text.slice(at, at + match.length)}
    </mark>
    {text.slice(at + match.length)}
  </>;
}
export function EventSearch({ hash, revision, isAdmin }: {
  hash: string;
  revision: number;
  isAdmin: boolean
}) {
  const mobile = useMediaQuery("(max-width:899px)");
  const params = new URLSearchParams(hash.split("?")[1] || "");
  const key = params.toString();
  const timeline = !isAdmin || params.get("timeline") === "true";
  const [data, setData] = useState<Page<Event>>({ rows: [], total: 0, has_more: false });
  const [busy, setBusy] = useState(true), [error, setError] = useState(""), [open, setOpen] = useState<number | null>(null);
  const [advanced, setAdvanced] = useState(!!params.get("report_id") || !!params.get("job_id") || !!params.get("request_id"));
  const [live, setLive] = useState(false), [users, setUsers] = useState<string[]>([]);
  const userInput = useRef<HTMLInputElement | null>(null), reportInput = useRef<HTMLInputElement | null>(null);
  function update(values: Record<string, string>) {
    const next = new URLSearchParams(params);
    Object.entries(values).forEach(([k, v]) => v ? next.set(k, v) : next.delete(k));
    navigate(`#/admin/${isAdmin ? "logs" : "audit"}?${next}`, true);
  }
  const effective = new URLSearchParams(params);
  if (!effective.get("date_from") && !(timeline && effective.get("report_id"))) effective.set("date_from", new Date(Date.now() - 86400000).toISOString());
  if (timeline) {
    effective.set("timeline", "true");
    effective.set("ascending", "true");
  }
  async function load(more = false) {
    setBusy(true);
    const p = new URLSearchParams(effective);
    p.set("offset", String(more ? data.rows.length : 0));
    try {
      const next = await accessCall<Page<Event>>(`${isAdmin ? "/admin/events" : "/group-admin/events"}?${p}`);
      setData(d => ({ ...next, rows: more ? [...d.rows, ...next.rows] : next.rows }));
      setError("");
    } catch (e) { setError((e as Error).message); } finally { setBusy(false); }
  }
  useEffect(
    () => {
      const t = setTimeout(() => void load(), 200);
      return () => clearTimeout(t);
    },
    [key, revision]
  );
  useEffect(
    () => {
      if (!live) return;
      const t = setInterval(() => void load(), 3000);
      return () => clearInterval(t);
    },
    [key, live]
  );
  const base = api.base.endsWith("/api") ? api.base : api.base + "/api";
  const exports = new URLSearchParams(effective);
  exports.set("export", "true");
  const levelValues = params.has("levels") ? (params.get("levels") || "").split(",").filter(Boolean) : defaultLevels;
  const filterCount = ['username', 'service', 'report_id', 'job_id', 'request_id', 'event_type'].filter(k => params.get(k)).length
    + Number(params.has('levels') && params.get('levels') !== defaultLevels.join(','))
    + ['include_http', 'timeline'].filter(k => params.get(k) === 'true').length
    + Number(params.has('date_from') || params.has('date_to'));
  return <Panel
    title={timeline ? uiText("События отчёта") : uiText("Логи сервиса")}
    actions={isAdmin && <Button
      color="inherit"
      variant="outlined"
      href={`${base}/admin/events?${exports}`}
    >{uiText("Экспорт CSV")}</Button>}
  >
    <AdminFilters
      search={<TextField
        label={uiText("Поиск по сообщению")}
        value={params.get("q") || ""}
        onChange={e => update({ q: e.target.value })}
      />}
      count={filterCount}
      columns={isAdmin ? 3 : 2}
      before={<Stack
        direction="row"
        gap={1}
        flexWrap="wrap"
        sx={{ mb: 2 }}
      >
        {[[uiText("15 мин"), 900], [uiText("1 час"), 3600], [uiText("24 часа"), 86400], [uiText("7 дней"), 604800]].map(
          ([label, seconds]) =>
            <Button
              key={label}
              color="inherit"
              variant="outlined"
              size="small"
              onClick={() => update(
                { date_from: new Date(Date.now() - Number(seconds) * 1000).toISOString(), date_to: "" }
              )}
            >
              {label}
            </Button>
        )}
      </Stack>}
      after={<>
        <Stack
          direction="row"
          justifyContent="space-between"
          alignItems="center"
          flexWrap="wrap"
          gap={1}
          sx={{ my: 1 }}
        >
          <Button
            color="inherit"
            startIcon={<FilterListIcon />}
            aria-expanded={advanced}
            onClick={() => setAdvanced(v => !v)}
          >{uiText("Ещё фильтры")}</Button>
          <FormControlLabel
            label={uiText("Обновлять в реальном времени")}
            control={<Switch
              size="small"
              checked={live}
              onChange={e => setLive(e.target.checked)}
            />}
          />
        </Stack>
        <Collapse in={advanced}>
          <Box
            className="admin-filter-row"
            sx={{ my: 2 }}
          >
            <TextField
              inputRef={reportInput}
              label={uiText("ID отчёта")}
              value={params.get("report_id") || ""}
              onChange={e => update({ report_id: e.target.value })}
            />
            {isAdmin && <>
              <TextField
                label={uiText("ID задания")}
                value={params.get("job_id") || ""}
                onChange={e => update({ job_id: e.target.value })}
              />
              <TextField
                label={uiText("ID запроса")}
                value={params.get("request_id") || ""}
                onChange={e => update({ request_id: e.target.value })}
              />
              <TextField
                label={uiText("Тип события")}
                value={params.get("event_type") || ""}
                onChange={e => update({ event_type: e.target.value })}
              />
              <FormControlLabel
                label={uiText("Показать HTTP-запросы")}
                control={<Switch
                  checked={params.get("include_http") === "true"}
                  onChange={e => update({ include_http: e.target.checked ? "true" : "" })}
                />}
              />
              <FormControlLabel
                label={uiText("Только хронология отчёта")}
                control={<Switch
                  checked={timeline}
                  onChange={e => update({ timeline: e.target.checked ? "true" : "" })}
                />}
              />
            </>}
          </Box>
        </Collapse>
        <Stack
          direction="row"
          gap={1}
          flexWrap="wrap"
          sx={{ mb: 2 }}
        >
          <Button
            size="small"
            color="inherit"
            onClick={() => update(
              {
                levels: "ERROR,CRITICAL",
                date_from: new Date(Date.now() - 86400000).toISOString(),
                date_to: ""
              }
            )}
          >{uiText("Ошибки за 24 ч")}</Button>
          <Button
            size="small"
            color="inherit"
            onClick={() => {
              update({ timeline: "true" });
              userInput.current?.focus();
            }}
          >{uiText("Действия пользователя…")}</Button>
          <Button
            size="small"
            color="inherit"
            onClick={() => {
              setAdvanced(true);
              setTimeout(() => reportInput.current?.focus(), 100);
            }}
          >{uiText("События отчёта…")}</Button>
          <Button
            size="small"
            color="inherit"
            onClick={() => navigate(`#/admin/${isAdmin ? "logs" : "audit"}`, true)}
          >{uiText("Сбросить фильтры")}</Button>
        </Stack>
      </>}
      onReset={() => navigate(`#/admin/${isAdmin ? "logs" : "audit"}`, true)}
    >
      <TextField
        type="datetime-local"
        label={uiText("С (МСК)")}
        value={localMoscow(effective.get("date_from"))}
        InputLabelProps={{ shrink: true }}
        onChange={e => update({ date_from: utcMoscow(e.target.value) })}
      />
      <TextField
        type="datetime-local"
        label={uiText("До (МСК)")}
        value={localMoscow(params.get("date_to"))}
        InputLabelProps={{ shrink: true }}
        onChange={e => update({ date_to: utcMoscow(e.target.value) })}
      />
      {isAdmin && <>
        <Autocomplete
          multiple
          options={["DEBUG", ...defaultLevels]}
          value={levelValues}
          onChange={(_, v) => update({ levels: v.join(",") })}
          renderTags={values => <Tooltip title={values.join(", ")}>
            <Typography
              variant="body2"
              noWrap
              sx={{ maxWidth: "calc(100% - 40px)", pl: .5 }}
            >
              {values.length === 4 && !values.includes("DEBUG") ? uiText("Без DEBUG") : values.join(", ")}
            </Typography>
          </Tooltip>}
          sx={{ "& .MuiAutocomplete-inputRoot": { flexWrap: "nowrap" } }}
          renderInput={p => <TextField
            {...p}
            label={uiText("Уровни")}
          />}
        />
        <TextField
          select
          label={uiText("Сервис")}
          value={params.get("service") || ""}
          onChange={e => update({ service: e.target.value })}
        >
          <MenuItem value="">{uiText("Все сервисы")}</MenuItem>
          {["backend", "worker", "inspect-worker"].map(
            v => <MenuItem
              key={v}
              value={v}
            >
              {v}
            </MenuItem>
          )}
        </TextField>
      </>}
      <Autocomplete
        freeSolo
        options={users}
        inputValue={params.get("username") || ""}
        onInputChange={(_, v) => {
          update({ username: v });
          if (v.length >= 2) void accessCall<Array<{
            username: string
          }>>(`/group-admin/users/search?q=${encodeURIComponent(v)}`).then(rows => setUsers(rows.map(u => u.username))).catch(() => setUsers([]));
        }}
        renderInput={p => <TextField
          {...p}
          inputRef={userInput}
          label={uiText("Пользователь: логин или ФИО")}
        />}
      />
    </AdminFilters>
    <Failure error={error} />
    <Typography
      variant="body2"
      color="text.secondary"
      sx={{ my: 1 }}
    >{uiText("Найдено:") + " "}{quantity(data.total)}{" " + uiText("· показано:") + " "}{quantity(data.rows.length)}</Typography>
    {busy && !data.rows.length ? <Loading /> : <TableContainer>
      <Table
        size="small"
        className="admin-table admin-log-table"
      >
        <TableHead>
          <TableRow>
            <TableCell />
            <TableCell>{uiText("Время МСК")}</TableCell>
            <TableCell>{uiText("Уровень / сервис")}</TableCell>
            <TableCell>{uiText("Пользователь")}</TableCell>
            <TableCell>{uiText("Сообщение")}</TableCell>
          </TableRow>
        </TableHead>
        <TableBody>
          {data.rows.map(
            r => <Fragment key={r.id}>
              <TableRow hover>
                <TableCell>
                  <IconButton
                    size="small"
                    aria-label={uiText("Раскрыть сообщение")}
                    aria-expanded={open === r.id}
                    onClick={() => setOpen(open === r.id ? null : r.id)}
                  >
                    {open === r.id ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                  </IconButton>
                </TableCell>
                <TableCell sx={{ whiteSpace: "nowrap" }}>
                  <span className="event-when-long">
                    {moscowDate(r.event_at, true)}
                  </span>
                  <Tooltip title={moscowDate(r.event_at, true)}>
                    <span className="event-when-short">
                      {moscowDate(r.event_at, true).slice(11, 19)}
                    </span>
                  </Tooltip>
                </TableCell>
                <TableCell>
                  <Typography
                    variant="caption"
                    className={`event-level level-${r.level.toLowerCase()}`}
                  >
                    {r.level}
                  </Typography>
                  <Typography
                    variant="caption"
                    color="text.secondary"
                    display="block"
                  >
                    {r.service || uiText("Событие")}
                  </Typography>
                </TableCell>
                <TableCell>
                  {r.username ? <Button
                    color="inherit"
                    className="admin-text-link"
                    onClick={() => update({ username: r.username! })}
                  >
                    {r.username}
                  </Button> : "—"}
                </TableCell>
                <TableCell
                  className="admin-log-message"
                  onClick={() => setOpen(open === r.id ? null : r.id)}
                >
                  <Tooltip title={uiText("Раскрыть сообщение")}>
                    <Typography
                      variant="body2"
                      className="admin-ellipsis"
                    >
                      <Highlight
                        text={r.message}
                        match={params.get("q") || ""}
                      />
                    </Typography>
                  </Tooltip>
                </TableCell>
              </TableRow>
              <TableRow>
                <TableCell
                  colSpan={mobile ? 3 : 5}
                  sx={{ p: 0, border: 0 }}
                >
                  <Collapse
                    in={open === r.id}
                    unmountOnExit
                  >
                    <Box className="admin-row-detail">
                      <Stack
                        direction="row"
                        gap={1}
                        flexWrap="wrap"
                      >
                        <Button
                          color="inherit"
                          startIcon={<ContentCopyIcon />}
                          onClick={() => void navigator.clipboard.writeText(r.message + (r.traceback ? "\n" + r.traceback : "")).catch(() => setError(uiText("Не удалось скопировать. Выделите текст сообщения.")))}
                        >{uiText("Копировать")}</Button>
                        {r.report_id && <Button
                          color="inherit"
                          onClick={() => navigate(
                            `#/admin/${isAdmin ? "logs" : "audit"}?${new URLSearchParams({ report_id: r.report_id!, timeline: "true" })}`,
                            true
                          )}
                        >{uiText("Хронология отчёта")}</Button>}
                      </Stack>
                      <Box
                        component="pre"
                        className="admin-log-full"
                      >
                        {r.message}
                        {r.traceback && "\n" + r.traceback}
                      </Box>
                      <Typography
                        variant="caption"
                        color="text.secondary"
                      >{r.event_type} · {r.job_id || uiText("Без задания")}{r.request_id && uiText("· запрос {0}", r.request_id)}</Typography>
                      {r.report_id && <Button
                        size="small"
                        color="inherit"
                        onClick={() => update({ report_id: r.report_id! })}
                      >{uiText("Фильтр по отчёту") + " "}{r.report_id}</Button>}
                    </Box>
                  </Collapse>
                </TableCell>
              </TableRow>
            </Fragment>
          )}
        </TableBody>
      </Table>
    </TableContainer>}
    {!busy && !data.rows.length && <Empty text={uiText("По этим условиям событий нет. Попробуйте расширить период.")} />}
    {data.has_more && <Button
      variant="outlined"
      color="inherit"
      disabled={busy}
      onClick={() => void load(true)}
    >{uiText("Показать ещё")}</Button>}
  </Panel>;
}
type Audit = {
  id: number;
  actor_username: string | null;
  actor_full_name?: string | null;
  created_at: string;
  action: string;
  object_label: string;
  description: string;
  changes: Array<{
    field: string;
    label: string;
    before: string;
    after: string
  }>
};
const actions: Record<string, string> = {
  "model.create": uiText("Модель добавлена"),
  "model.update": uiText("Настройки модели"),
  "model.delete": uiText("Модель удалена"),
  "model.check": uiText("Модель проверена"),
  "group.create": uiText("Создана группа"),
  "group.update": uiText("Настройки группы"),
  "member.save": uiText("Участник изменён"),
  "member.add": uiText("Участник добавлен"),
  "member.remove": uiText("Участник исключён"),
  "member.role": uiText("Роль в группе"),
  "user.role": uiText("Роль пользователя"),
  "request.create": uiText("Заявка подана"),
  "request.approved": uiText("Заявка одобрена"),
  "request.rejected": uiText("Заявка отклонена"),
  "request.canceled": uiText("Заявка отменена"),
  "report.delete": uiText("Отчёт удалён")
};
export function AuditSearch({ hash, revision, isAdmin }: {
  hash: string;
  revision: number;
  isAdmin: boolean
}) {
  const params = new URLSearchParams(hash.split("?")[1] || ""), key = params.toString();
  const [data, setData] = useState<Page<Audit>>({ rows: [], total: 0, has_more: false });
  const [busy, setBusy] = useState(true), [error, setError] = useState("");
  function update(k: string, v: string) {
    const p = new URLSearchParams(params);
    v ? p.set(k, v) : p.delete(k);
    navigate(`#/admin/audit?${p}`, true);
  }
  async function load(more = false) {
    setBusy(true);
    const p = new URLSearchParams(params);
    p.set("offset", String(more ? data.rows.length : 0));
    try {
      const next = await accessCall<Page<Audit>>(`/group-admin/audit?${p}`);
      setData(d => ({ ...next, rows: more ? [...d.rows, ...next.rows] : next.rows }));
      setError("");
    }
    catch (e) { setError((e as Error).message); } finally { setBusy(false); }
  }
  useEffect(
    () => {
      const t = setTimeout(() => void load(), 200);
      return () => clearTimeout(t);
    },
    [key, revision]
  );
  if (params.get("report_id")) return <EventSearch
    hash={hash}
    revision={revision}
    isAdmin={isAdmin}
  />;
  return <Panel title={isAdmin ? uiText("Журнал изменений") : uiText("Журнал своих групп")}>
    <AdminFilters
      search={<TextField
        label={uiText("Поиск по объекту")}
        title={uiText("Название группы, отчёта или логин участника")}
        value={params.get("object_query") || ""}
        onChange={e => update("object_query", e.target.value)}
      />}
      count={['author', 'action'].filter(k => params.get(k)).length + Number(params.get('include_unchanged') === 'true') + Number(params.has('date_from') || params.has('date_to'))}
      onReset={() => navigate("#/admin/audit", true)}
    >
      <TextField
        label={uiText("Автор: логин или ФИО")}
        value={params.get("author") || ""}
        onChange={e => update("author", e.target.value)}
      />
      <TextField
        select
        label={uiText("Действие")}
        value={params.get("action") || ""}
        onChange={e => update("action", e.target.value)}
      >
        <MenuItem value="">{uiText("Все действия")}</MenuItem>
        {Object.entries(actions).map(
          ([v, label]) => <MenuItem
            key={v}
            value={v}
          >
            {label}
          </MenuItem>
        )}
      </TextField>
      <TextField
        type="datetime-local"
        label={uiText("С (МСК)")}
        InputLabelProps={{ shrink: true }}
        value={localMoscow(params.get("date_from"))}
        onChange={e => update("date_from", utcMoscow(e.target.value))}
      />
      <TextField
        type="datetime-local"
        label={uiText("До (МСК)")}
        InputLabelProps={{ shrink: true }}
        value={localMoscow(params.get("date_to"))}
        onChange={e => update("date_to", utcMoscow(e.target.value))}
      />
      <FormControlLabel
        label={uiText("Показать записи без изменений")}
        control={<Switch
          checked={params.get("include_unchanged") === "true"}
          onChange={e => update("include_unchanged", e.target.checked ? "true" : "")}
        />}
      />
    </AdminFilters>
    <Failure error={error} />
    <Typography
      variant="body2"
      color="text.secondary"
      sx={{ my: 2 }}
    >{uiText("Найдено:") + " "}{quantity(data.total)}</Typography>
    {busy && !data.rows.length ? <Loading /> : <Stack
      component="ol"
      className="admin-audit-feed"
      gap={0}
    >
      {data.rows.map(
        r => <Box
          component="li"
          key={r.id}
          className="admin-audit-row"
        >
          <Stack
            direction="row"
            gap={1}
            alignItems="center"
            flexWrap="wrap"
          >
            <Typography
              fontWeight={600}
              variant="body2"
            >
              {actions[r.action] || r.action}
            </Typography>
            <Typography
              variant="caption"
              color="text.secondary"
            >
              {moscowDate(r.created_at, true)}
            </Typography>
            <Button
              color="inherit"
              className="admin-text-link"
              size="small"
              onClick={() => update("author", r.actor_username || "")}
            >
              <span>{r.actor_full_name || r.actor_username || uiText("Система")}
                {r.actor_full_name && <Typography component="span" display="block" variant="caption" color="text.secondary">{r.actor_username}</Typography>}
              </span>
            </Button>
          </Stack>
          <Typography
            variant="body2"
            sx={{ mt: .5 }}
          >
            {r.object_label}
          </Typography>
          <Typography
            variant="body2"
            color="text.secondary"
            sx={{ mt: .5, overflowWrap: "anywhere" }}
          >
            {r.description}
          </Typography>
        </Box>
      )}
    </Stack>}
    {!busy && !data.rows.length && <Empty
      text={uiText("Изменений по этим условиям нет")}
      action={<Button
        color="inherit"
        onClick={() => navigate("#/admin/audit", true)}
      >{uiText("Сбросить фильтры")}</Button>}
    />}
    {data.has_more && <Button
      variant="outlined"
      color="inherit"
      disabled={busy}
      onClick={() => void load(true)}
    >{uiText("Показать ещё")}</Button>}
  </Panel>;
}
