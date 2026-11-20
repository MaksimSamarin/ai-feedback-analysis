import { uiText } from "./i18n";
import { UserIdentity, type Profile } from "./userIdentity";
import { AdminFilters } from "./adminFilters";
import { useEffect, useState, type MouseEvent } from "react";
import {
  Autocomplete,
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Drawer,
  IconButton,
  Menu,
  MenuItem,
  Stack,
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
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import DownloadIcon from "@mui/icons-material/Download";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import CloseIcon from "@mui/icons-material/Close";
import { api } from "./api";
import { accessCall, type AccessGroup } from "./access";
import type { ReportAnalysis } from "./types";
import {
  Empty,
  Failure,
  Loading,
  Pager,
  Panel,
  StateChip,
  moscowDate,
  quantity,
  utcMoscow
} from "./adminShared";
export type ReportMetadata = Profile & {
  id: string;
  name: string | null;
  user_id: number;
  username: string;
  access_group_id: number | null;
  group_name: string | null;
  status: string;
  status_label?: string;
  all_rows_failed?: boolean;
  has_errors?: boolean;
  retry_hint?: string | null;
  created_at: string;
  updated_at: string;
  deleted_at: string | null;
  total_tokens: number;
  total_rows: number;
  processed_rows: number;
  failed_rows?: number | null;
  error_code: string | null
};
type Page = {
  rows: ReportMetadata[];
  total: number;
  has_more: boolean
};
type Event = {
  id: number;
  event_at: string;
  message: string;
  username: string | null;
  event_type: string
};
export function ReportTimeline({ reportId, revision = 0 }: {
  reportId: string;
  revision?: number
}) {
  const [rows, setRows] = useState<Event[]>([]), [error, setError] = useState(""), [busy, setBusy] = useState(true);
  useEffect(
    () => {
      setBusy(true);
      void accessCall<{
        rows: Event[]
      }>(`/group-admin/events?report_id=${encodeURIComponent(reportId)}&limit=500`)
        .then(d => {
          setRows(d.rows);
          setError("");
        }).catch(e => setError(e.message)).finally(() => setBusy(false));
    },
    [reportId, revision]
  );
  return <>
    <Typography
      variant="h6"
      sx={{ mb: 2 }}
    >{uiText("Хронология")}</Typography>
    <Failure error={error} />
    {busy ? <Loading /> : rows.length ? <Box
      component="ol"
      className="admin-timeline"
    >
      {rows.map(
        r => <Box
          component="li"
          key={r.id}
        >
          <Typography
            variant="caption"
            color="text.secondary"
          >{moscowDate(r.event_at, true)} · {r.username || uiText("Система")}</Typography>
          <Typography variant="body2">
            {r.message}
          </Typography>
        </Box>
      )}
    </Box> : <Empty text={uiText("Событий этого отчёта пока нет")} />}
  </>;
}
export function Reports({ isAdmin, revision, hash, groupId, userId, period, groups = [] }: {
  isAdmin: boolean;
  revision: number;
  hash?: string;
  groupId?: number;
  userId?: number;
  period?: {
    from: string;
    to: string
  };
  groups?: AccessGroup[];
}) {
  const mobile = useMediaQuery("(max-width:699px)");
  const [data, setData] = useState<Page>({ rows: [], total: 0, has_more: false });
  const [query, setQuery] = useState(""), [group, setGroup] = useState(""), [status, setStatus] = useState("");
  const [from, setFrom] = useState(""), [to, setTo] = useState("");
  const [who, setWho] = useState<{
    id: number;
    username: string
  } | null>(null), [options, setOptions] = useState<typeof who[]>([]);
  const [page, setPage] = useState(0), [size, setSize] = useState(25), [busy, setBusy] = useState(true), [error, setError] = useState("");
  const [selected, setSelected] = useState<ReportMetadata | null>(null), [menuRow, setMenuRow] = useState<ReportMetadata | null>(null);
  const [anchor, setAnchor] = useState<HTMLElement | null>(null), [remove, setRemove] = useState<ReportMetadata | null>(null);
  const [analysis, setAnalysis] = useState<ReportAnalysis | null>(null), [localRevision, setLocalRevision] = useState(0);
  const [showResults, setShowResults] = useState(false);
  const routeStatus = new URLSearchParams(hash?.split("?")[1] || "").get("status") || "";
  useEffect(() => {
    setAnalysis(null);
    setShowResults(false);
  }, [selected?.id]);
  useEffect(
    () => {
      if (!isAdmin || !selected?.all_rows_failed) return;
      let active = true;
      void api.adminReportAnalysis(selected.id).then(result => { if (active) setAnalysis(result); })
        .catch(e => { if (active) setError(e.message); });
      return () => { active = false; };
    },
    [isAdmin, selected?.id, selected?.all_rows_failed]
  );
  useEffect(
    () => {
      setSelected(current => current ? data.rows.find(row => row.id === current.id) || current : null);
    },
    [data.rows]
  );
  useEffect(() => {
    setStatus(routeStatus);
    setPage(0);
  }, [routeStatus]);
  useEffect(
    () => {
      let active = true;
      setBusy(true);
      const p = new URLSearchParams({ offset: String(page * size), limit: String(size), include_test: "false" });
      if (groupId !== undefined) p.set("group_id", String(groupId)); else if (group) p.set("group_id", group);
      if (userId) p.set("user_id", String(userId)); else if (who) p.set("user_id", String(who.id));
      if (query) p.set("search", query);
      if (status) p.set("status", status);
      if (period) {
        p.set("date_from", period.from);
        p.set("date_to", period.to);
      }
      else {
        if (from) p.set("date_from", utcMoscow(from));
        if (to) p.set("date_to", utcMoscow(to));
      }
      const timer = window.setTimeout(
        () => void accessCall<Page>(`/group-admin/reports?${p}`)
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
    [
      revision,
      localRevision,
      groupId,
      userId,
      period?.from,
      period?.to,
      query,
      group,
      who,
      status,
      from,
      to,
      page,
      size
    ]
  );
  const base = api.base.endsWith("/api") ? api.base : api.base + "/api";
  const download = (r: ReportMetadata, kind: string) => `${base}/admin/reports/${r.id}/download/${kind}`;
  const xlsx = (r: ReportMetadata) => download(r, ["queued", "running", "paused"].includes(r.status) ? "partial/xlsx" : "xlsx");
  function more(e: MouseEvent<HTMLElement>, row: ReportMetadata) {
    e.stopPropagation();
    setAnchor(e.currentTarget);
    setMenuRow(row);
  }
  const actions = (row: ReportMetadata) => <Stack
    direction="row"
    gap={.5}
    onClick={e => e.stopPropagation()}
  >
    <Tooltip title={uiText("Открыть карточку")}>
      <IconButton
        aria-label={uiText("Открыть {0}", row.name || row.id)}
        onClick={() => setSelected(row)}
      >
        <OpenInNewIcon fontSize="small" />
      </IconButton>
    </Tooltip>
    {isAdmin && !row.deleted_at && <Tooltip title={uiText("Скачать XLSX")}>
      <IconButton
        component="a"
        href={xlsx(row)}
        aria-label={uiText("Скачать XLSX")}
      >
        <DownloadIcon fontSize="small" />
      </IconButton>
    </Tooltip>}
    <Tooltip title={uiText("Другие действия")}>
      <IconButton
        aria-label={uiText("Другие действия отчёта")}
        onClick={e => more(e, row)}
      >
        <MoreHorizIcon />
      </IconButton>
    </Tooltip>
  </Stack>;
  async function action(row: ReportMetadata, name: "pause" | "resume" | "cancel" | "delete") {
    try {
      await ({
        pause: api.adminPauseReport,
        resume: api.adminResumeReport,
        cancel: api.adminCancelReport,
        delete: api.adminDeleteReport
      }[name])(row.id);
      setLocalRevision(v => v + 1);
      if (name === "delete") setSelected(null);
    } catch (e) { setError((e as Error).message); }
  }
  return <Panel title={uiText("Отчёты пользователей · {0}", quantity(data.total))}>
    <AdminFilters
      search={<TextField
        label={uiText("Название или логин")}
        value={query}
        onChange={e => {
          setQuery(e.target.value);
          setPage(0);
        }}
      />}
      count={[group, who, status, from, to].filter(Boolean).length}
      onReset={() => {
        setGroup("");
        setWho(null);
        setStatus("");
        setFrom("");
        setTo("");
        setPage(0);
      }}
    >
      {!userId && <Autocomplete
        options={options.filter((v): v is NonNullable<typeof who> => v !== null)}
        value={who}
        getOptionLabel={v => v.username}
        isOptionEqualToValue={(a, b) => a.id === b.id}
        onChange={(_, v) => {
          setWho(v);
          setPage(0);
        }}
        onInputChange={(_, v) => {
          if (v.length >= 2) void accessCall<NonNullable<typeof who>[]>(`/group-admin/users/search?q=${encodeURIComponent(v)}`)
            .then(setOptions).catch(e => setError(e.message));
        }}
        renderInput={p => <TextField
          {...p}
          label={uiText("Пользователь")}
        />}
      />}
      {groupId === undefined && <TextField
        select
        label={uiText("Группа")}
        value={group}
        onChange={e => {
          setGroup(e.target.value);
          setPage(0);
        }}
      >
        <MenuItem value="">{uiText("Все доступные")}</MenuItem>
        {isAdmin && <MenuItem value="0">{uiText("Без группы")}</MenuItem>}
        {groups.map(
          g => <MenuItem
            key={g.id}
            value={String(g.id)}
          >
            {g.name}
          </MenuItem>
        )}
      </TextField>}
      <TextField
        select
        label={uiText("Статус")}
        value={status}
        onChange={e => {
          setStatus(e.target.value);
          setPage(0);
        }}
      >
        {[
          ["", uiText("Все статусы")],
          ["queued", uiText("В очереди")],
          ["running", uiText("В работе")],
          ["paused", uiText("На паузе")],
          ["completed", uiText("Завершены")],
          ["failed", uiText("Ошибка")],
          ["errors", uiText("С ошибками строк")],
          ["canceled", uiText("Отменены")]
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
      {!period && <>
        <TextField
          label={uiText("С (МСК)")}
          type="datetime-local"
          InputLabelProps={{ shrink: true }}
          value={from}
          onChange={e => {
            setFrom(e.target.value);
            setPage(0);
          }}
        />
        <TextField
          label={uiText("До (МСК)")}
          type="datetime-local"
          InputLabelProps={{ shrink: true }}
          value={to}
          onChange={e => {
            setTo(e.target.value);
            setPage(0);
          }}
        />
      </>}
    </AdminFilters>
    <Failure error={error} />
    {busy ? <Loading /> : mobile ? <Stack gap={1}>
      {data.rows.map(
        r => <Box
          key={r.id}
          className="admin-mobile-card"
          role="button"
          tabIndex={0}
          onClick={() => setSelected(r)}
          onKeyDown={e => e.key === "Enter" && setSelected(r)}
        >
          <Typography fontWeight={600}>
            {r.name || uiText("Без названия")}
          </Typography>
          <Typography
            variant="body2"
            color="text.secondary"
          ><UserIdentity user={r} /> · {r.group_name || uiText("Без группы")}</Typography>
          <Box sx={{ my: 1 }}>
            <StateChip row={r} />
          </Box>
          <Typography variant="body2">{quantity(r.processed_rows)}{" " + uiText("строк ·") + " "}{quantity(r.total_tokens)}{" " + uiText("токенов")}</Typography>
          <Stack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
          >
            <Typography variant="caption">
              {moscowDate(r.created_at)}
            </Typography>
            {actions(r)}
          </Stack>
        </Box>
      )}
    </Stack> : <TableContainer>
      <Table
        size="small"
        className="admin-table"
      >
        <TableHead>
          <TableRow>
            {[uiText("Отчёт"), uiText("Пользователь / группа"), uiText("Создан"), uiText("Статус"), uiText("Строки"), uiText("Токены"), ""].map(h => <TableCell key={h}>
              {h}
            </TableCell>)}
          </TableRow>
        </TableHead>
        <TableBody>
          {data.rows.map(
            r => <TableRow
              key={r.id}
              hover
              onClick={() => setSelected(r)}
              className="admin-click-row"
            >
              <TableCell sx={{ maxWidth: 230 }}>
                <Tooltip title={r.name || r.id}>
                  <Typography
                    variant="body2"
                    className="admin-ellipsis"
                  >
                    {r.name || uiText("Без названия")}
                  </Typography>
                </Tooltip>
              </TableCell>
              <TableCell>
                <UserIdentity user={r} />
                <Typography
                  variant="caption"
                  display="block"
                  color="text.secondary"
                >
                  {r.group_name || uiText("Без группы")}
                </Typography>
              </TableCell>
              <TableCell>
                {moscowDate(r.created_at)}
              </TableCell>
              <TableCell>
                <StateChip row={r} />
              </TableCell>
              <TableCell align="right">
                {quantity(r.processed_rows)}
              </TableCell>
              <TableCell align="right">
                {quantity(r.total_tokens)}
              </TableCell>
              <TableCell>
                {actions(r)}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </TableContainer>}
    {!busy && !data.rows.length && <Empty
      text={uiText("Отчётов по этим условиям нет")}
      action={<Button
        onClick={() => {
          setQuery("");
          setGroup("");
          setWho(null);
          setStatus("");
          setFrom("");
          setTo("");
          setPage(0);
        }}
      >{uiText("Сбросить фильтры")}</Button>}
    />}
    <Pager
      total={data.total}
      page={page}
      size={size}
      setPage={setPage}
      setSize={setSize}
    />
    <Menu
      anchorEl={anchor}
      open={!!anchor}
      onClose={() => setAnchor(null)}
    >
      {isAdmin && menuRow && !menuRow.deleted_at && ["raw", "source"].map(
        kind => <MenuItem
          key={kind}
          component="a"
          href={download(menuRow, kind)}
          onClick={() => setAnchor(null)}
        >
          {kind === "raw" ? uiText("Скачать RAW") : uiText("Скачать исходный файл")}
        </MenuItem>
      )}
      <MenuItem onClick={() => {
        setSelected(menuRow);
        setAnchor(null);
      }}>{uiText("События отчёта")}</MenuItem>
      {isAdmin && menuRow && !menuRow.deleted_at && !["running", "queued", "paused"].includes(menuRow.status) && <MenuItem
        sx={{ color: "error.main" }}
        onClick={() => {
          setRemove(menuRow);
          setAnchor(null);
        }}
      >{uiText("Удалить отчёт")}</MenuItem>}
    </Menu>
    <Drawer
      anchor="right"
      open={!!selected}
      onClose={() => {
        setSelected(null);
        setAnalysis(null);
      }}
      PaperProps={{ className: "admin-page", sx: { width: { xs: "100%", md: 640 }, maxWidth: "100%" } }}
    >
      {selected && <Box sx={{ p: 3 }}>
        <Stack
          direction="row"
          justifyContent="space-between"
          alignItems="center"
          gap={1}
        >
          <Typography variant="h6">
            {selected.name || uiText("Отчёт без названия")}
          </Typography>
          <IconButton
            aria-label={uiText("Закрыть карточку")}
            onClick={() => setSelected(null)}
          >
            <CloseIcon />
          </IconButton>
        </Stack>
        <Typography
          color="text.secondary"
          variant="body2"
        ><UserIdentity user={selected} /> · {selected.group_name || uiText("Без группы")}</Typography>
        <Box sx={{ my: 2 }}>
          <StateChip row={selected} />
        </Box>
        <Typography>{quantity(selected.processed_rows)}{" " + uiText("из") + " "}{quantity(selected.total_rows)}{" " + uiText("строк ·") + " "}{quantity(selected.total_tokens)}{" " + uiText("токенов")}</Typography>
        <Typography
          variant="body2"
          sx={{ mt: 1 }}
        >
          {selected.retry_hint || (selected.has_errors ? uiText("Скачайте список ошибок и повторите обработку.") : "")}
        </Typography>
        {selected.all_rows_failed && analysis?.preview_rows.some(row => row.error) &&
          <Alert
            severity="error"
            sx={{ my: 2 }}
          >{uiText("Первая ошибка:") + " "}{analysis.preview_rows.find(row => row.error)?.error}
          </Alert>}
        <Typography
          variant="caption"
          color="text.secondary"
        >{uiText("Создан") + " "}{moscowDate(selected.created_at)}</Typography>
        {isAdmin && !selected.deleted_at && <Stack
          direction="row"
          gap={1}
          flexWrap="wrap"
          sx={{ my: 2 }}
        >
          <Button
            variant="outlined"
            href={xlsx(selected)}
          >XLSX</Button>
          <Button
            variant="outlined"
            onClick={() => {
              setShowResults(true);
              void api.adminReportAnalysis(selected.id).then(setAnalysis).catch(e => setError(e.message));
            }}
          >{uiText("Результаты")}</Button>
          {selected.status === "running" && <Button onClick={() => void action(selected, "pause")}>{uiText("Пауза")}</Button>}
          {selected.status === "paused" && <Button onClick={() => void action(selected, "resume")}>{uiText("Продолжить")}</Button>}
          {["queued", "running", "paused"].includes(selected.status) && <Button
            color="error"
            onClick={() => void action(selected, "cancel")}
          >{uiText("Отменить обработку")}</Button>}
        </Stack>}
        <Failure error={error} />
        {showResults && analysis && <TableContainer sx={{ my: 2 }}>
          <Table size="small">
            <TableBody>
              {analysis.preview_rows.map(
                r => <TableRow key={r.row_number}>
                  <TableCell>
                    {r.row_number}
                  </TableCell>
                  <TableCell>
                    {Object.entries(r.columns).map(
                      ([k, v]) => <Typography
                        key={k}
                        variant="body2"
                      >{k}: {String(v ?? "—")}</Typography>
                    )}
                  </TableCell>
                  <TableCell>
                    {r.error || "—"}
                  </TableCell>
                </TableRow>
              )}
            </TableBody>
          </Table>
        </TableContainer>}
        <Box sx={{ mt: 3 }}>
          <ReportTimeline
            reportId={selected.id}
            revision={localRevision}
          />
        </Box>
      </Box>}
    </Drawer>
    <Dialog
      open={!!remove}
      onClose={() => setRemove(null)}
    >
      <DialogTitle>{uiText("Удалить отчёт «")}{remove?.name || uiText("Без названия")}»?</DialogTitle>
      <DialogContent>{uiText("Результаты и файлы будут удалены. Учтённый расход сохранится.")}</DialogContent>
      <DialogActions>
        <Button onClick={() => setRemove(null)}>{uiText("Отмена")}</Button>
        <Button
          color="error"
          variant="contained"
          onClick={() => {
            if (remove) void action(remove, "delete");
            setRemove(null);
          }}
        >{uiText("Удалить")}</Button>
      </DialogActions>
    </Dialog>
  </Panel>;
}
