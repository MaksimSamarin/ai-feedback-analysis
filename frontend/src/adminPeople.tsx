import { uiText } from "./i18n";
import { UserIdentity, ProfileDetails, type Profile } from "./userIdentity";
import { useEffect, useState } from "react";
import {
  Alert,
  Autocomplete,
  Box,
  Button,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  IconButton,
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
  TextField,
  Tooltip,
  Typography,
  useMediaQuery
} from "@mui/material";
import EditIcon from "@mui/icons-material/Edit";
import CheckIcon from "@mui/icons-material/Check";
import CloseIcon from "@mui/icons-material/Close";
import { accessCall, type AccessGroup, type AccessRequest } from "./access";
import {
  Empty,
  Failure,
  Loading,
  Pager,
  Panel,
  moscowDate,
  quantity
} from "./adminShared";
type ManagedUser = Profile & {
  id: number;
  username: string;
  role: string;
  effective_role: string;
  role_from_env: boolean;
  total_tokens: number;
  last_activity: string | null;
  groups: Array<{
    group_id: number;
    name: string;
    role_in_group: string;
    is_active: boolean
  }>
};
const roleNames: Record<string, string> = { admin: uiText("Администратор"), manager: uiText("Руководитель"), user: uiText("Пользователь") };
export function Users({ groups, revision, changed }: {
  groups: AccessGroup[];
  revision: number;
  changed: () => void
}) {
  const mobile = useMediaQuery("(max-width:699px)");
  const [rows, setRows] = useState<ManagedUser[]>([]), [total, setTotal] = useState(0);
  const [query, setQuery] = useState(""), [filterRole, setFilterRole] = useState(""), [group, setGroup] = useState("");
  const [page, setPage] = useState(0), [size, setSize] = useState(25), [busy, setBusy] = useState(true);
  const [edit, setEdit] = useState<ManagedUser | null>(null), [role, setRole] = useState("user");
  const [assigned, setAssigned] = useState<AccessGroup[]>([]), [error, setError] = useState(""), [saving, setSaving] = useState(false);
  useEffect(
    () => {
      let active = true;
      setBusy(true);
      const p = new URLSearchParams({ paged: "true", offset: String(page * size), limit: String(size) });
      if (query) p.set("search", query);
      if (filterRole) p.set("role_filter", filterRole);
      if (group) p.set("group_id", group);
      const timer = window.setTimeout(
        () => void accessCall<{
          rows: ManagedUser[];
          total: number
        }>(`/group-admin/users?${p}`)
          .then(
            d => {
              if (active) {
                setRows(d.rows);
                setTotal(d.total);
                setError("");
              }
            }
          )
          .catch(e => active && setError(e.message)).finally(() => active && setBusy(false)),
        200
      );
      return () => {
        active = false;
        clearTimeout(timer);
      };
    },
    [query, filterRole, group, page, size, revision]
  );
  function open(u: ManagedUser) {
    setEdit(u);
    setRole(u.effective_role);
    setAssigned(
      groups.filter(
        g => u.groups.some(
          m => m.group_id === g.id && m.role_in_group === "manager" && m.is_active)
      )
    );
    setError("");
  }
  const effective = (u: ManagedUser) => u.effective_role === "manager"
    ? uiText("Руководитель (группы: {0})", u.groups.filter(g => g.is_active && g.role_in_group === "manager").map(g => g.name).join(", "))
    : roleNames[u.effective_role] || uiText("Пользователь");
  const editButton = (u: ManagedUser) => <Tooltip title={uiText("Назначить роль")}>
    <IconButton
      aria-label={uiText("Роль $<UserIdentity user={u} />")}
      onClick={() => open(u)}
    >
      <EditIcon fontSize="small" />
    </IconButton>
  </Tooltip>;
  return <Panel title={uiText("Пользователи · {0}", quantity(total))}>
    <Box className="admin-filter-row">
      <TextField
        label={uiText("Поиск по ФИО или логину")}
        value={query}
        onChange={e => {
          setQuery(e.target.value);
          setPage(0);
        }}
      />
      <TextField
        select
        label={uiText("Роль")}
        value={filterRole}
        onChange={e => {
          setFilterRole(e.target.value);
          setPage(0);
        }}
      >
        <MenuItem value="">{uiText("Все роли")}</MenuItem>
        {Object.entries(roleNames).map(
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
          setGroup(e.target.value);
          setPage(0);
        }}
      >
        <MenuItem value="">{uiText("Все группы")}</MenuItem>
        {groups.map(
          g => <MenuItem
            key={g.id}
            value={String(g.id)}
          >
            {g.name}
          </MenuItem>
        )}
      </TextField>
    </Box>
    <Failure error={error} />
    {busy ? <Loading /> : mobile ? <Stack gap={1}>
      {rows.map(
        u => <Box
          key={u.id}
          className="admin-mobile-card"
        >
          <Stack
            direction="row"
            justifyContent="space-between"
          >
            <Typography fontWeight={600}>
              <UserIdentity user={u} />
            </Typography>
            {editButton(u)}
          </Stack>
          <Typography variant="body2">
            {effective(u)}
          </Typography>
          <Typography
            variant="body2"
            color="text.secondary"
          >
            {u.groups.map(g => `${g.name} · ${g.role_in_group === "manager" ? uiText("Руководитель") : uiText("Пользователь")}${g.is_active ? "" : " " + uiText("(отключена)")}`).join(", ") || uiText("Без групп")}
          </Typography>
          <Typography variant="body2">{quantity(u.total_tokens)}{" " + uiText("токенов · активность") + " "}{moscowDate(u.last_activity)}</Typography>
        </Box>
      )}
    </Stack> : <TableContainer>
      <Table
        size="small"
        className="admin-table"
      >
        <TableHead>
          <TableRow>
            {[uiText("Пользователь"), uiText("Эффективная роль"), uiText("Группы"), uiText("Последняя активность"), uiText("Токены"), ""].map(h => <TableCell key={h}>
              {h}
            </TableCell>)}
          </TableRow>
        </TableHead>
        <TableBody>
          {rows.map(
            u => <TableRow
              key={u.id}
              hover
            >
              <TableCell sx={{ fontWeight: 600 }}>
                <UserIdentity user={u} />
              </TableCell>
              <TableCell sx={{ maxWidth: 280 }}>
                {effective(u)}
                {" "}
                {u.role_from_env && <Chip
                  label={uiText("Единый вход")}
                  size="small"
                  variant="outlined"
                />}
              </TableCell>
              <TableCell>
                {u.groups.map(g => `${g.name} · ${g.role_in_group === "manager" ? uiText("Руководитель") : uiText("Пользователь")}${g.is_active ? "" : " " + uiText("(отключена)")}`).join(", ") || "—"}
              </TableCell>
              <TableCell>
                {moscowDate(u.last_activity)}
              </TableCell>
              <TableCell align="right">
                {quantity(u.total_tokens)}
              </TableCell>
              <TableCell align="right">
                {editButton(u)}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </TableContainer>}
    {!busy && !rows.length && <Empty
      text={uiText("По этим условиям пользователей нет")}
      action={<Button
        onClick={() => {
          setQuery("");
          setFilterRole("");
          setGroup("");
        }}
      >{uiText("Сбросить фильтры")}</Button>}
    />}
    <Pager
      total={total}
      page={page}
      size={size}
      setPage={setPage}
      setSize={setSize}
    />
    <Dialog
      open={!!edit}
      onClose={() => !saving && setEdit(null)}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle>{uiText("Роль пользователя") + " "}{edit?.username}</DialogTitle>
      <DialogContent>
        <ProfileDetails user={edit} />
        <Stack
          gap={2}
          sx={{ pt: 1 }}
        >
          <TextField
            select
            label={uiText("Роль")}
            value={role}
            onChange={e => setRole(e.target.value)}
          >
            {Object.entries(roleNames).map(
              ([v, label]) => <MenuItem
                key={v}
                value={v}
              >
                {label}
              </MenuItem>
            )}
          </TextField>
          {role === "manager" && <Autocomplete
            multiple
            options={groups.filter(g => g.is_active)}
            value={assigned}
            getOptionLabel={g => g.name}
            isOptionEqualToValue={(a, b) => a.id === b.id}
            onChange={(_, values) => setAssigned(values)}
            renderInput={p => <TextField
              {...p}
              label={uiText("Управляемые группы")}
            />}
          />}
          {edit?.role_from_env && <Alert severity="info">{uiText("Роль закреплена правилами единого входа.")}</Alert>}
          <Failure error={error} />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={() => setEdit(null)}>{uiText("Отмена")}</Button>
        <Button
          variant="contained"
          disabled={saving || (role === "manager" && !assigned.length)}
          onClick={async () => {
            setSaving(true);
            try {
              await accessCall(
                `/group-admin/users/${edit?.id}/effective-role`,
                "PATCH",
                {
                  role, group_ids: assigned.map(g => g.id),
                }
              );
              setEdit(null);
              changed();
            } catch (e) { setError((e as Error).message); } finally { setSaving(false); }
          }}
        >{uiText("Сохранить")}</Button>
      </DialogActions>
    </Dialog>
  </Panel>;
}
export type RequestRow = AccessRequest & {
  created_at?: string;
  decided_at?: string | null;
  decided_username?: string | null
};
const requestLabels: Record<string, string> = {
  pending: uiText("Ожидает решения"),
  approved: uiText("Одобрена"),
  rejected: uiText("Отклонена"),
  canceled: uiText("Отменена")
};
export function Requests({ rows, groups, changed, compact = false }: {
  rows: RequestRow[];
  groups: AccessGroup[];
  changed: () => void;
  compact?: boolean;
}) {
  const mobile = useMediaQuery("(max-width:699px)");
  const [tab, setTab] = useState(0), [page, setPage] = useState(0), [size, setSize] = useState(10);
  const [edit, setEdit] = useState<RequestRow | null>(null), [approve, setApprove] = useState(true);
  const [groupId, setGroupId] = useState<number | "">(""), [comment, setComment] = useState("");
  const [saving, setSaving] = useState(false), [error, setError] = useState("");
  const pending = rows.filter(r => r.status === "pending");
  const filtered = compact ? pending.slice(0, 4) : tab === 0 ? pending : rows.filter(r => r.status !== "pending");
  const visible = compact ? filtered : filtered.slice(page * size, (page + 1) * size);
  function open(row: RequestRow, decision: boolean) {
    setEdit(row);
    setApprove(decision);
    setGroupId(row.requested_group_id || "");
    setComment("");
    setError("");
  }
  const actions = (r: RequestRow) => r.status === "pending" && <Stack
    direction="row"
    gap={.5}
  >
    <Tooltip title={uiText("Одобрить")}>
      <IconButton
        color="success"
        aria-label={uiText("Одобрить $<UserIdentity user={r} /><ProfileDetails user={r} />")}
        onClick={() => open(r, true)}
      >
        <CheckIcon />
      </IconButton>
    </Tooltip>
    <Tooltip title={uiText("Отклонить")}>
      <IconButton
        color="error"
        aria-label={uiText("Отклонить $<UserIdentity user={r} /><ProfileDetails user={r} />")}
        onClick={() => open(r, false)}
      >
        <CloseIcon />
      </IconButton>
    </Tooltip>
  </Stack>;
  const content = <>
    {!compact && <Tabs
      value={tab}
      onChange={(_, value) => {
        setTab(value);
        setPage(0);
      }}
      sx={{ mb: 2 }}
    >
      <Tab label={uiText("Ожидают ({0})", pending.length)} />
      <Tab label={uiText("Обработанные")} />
    </Tabs>}
    {mobile || compact ? <Stack gap={1}>
      {visible.map(
        r => <Box
          key={r.id}
          className="admin-mobile-card"
        >
          <Stack
            direction="row"
            gap={1}
            justifyContent="space-between"
          >
            <Box sx={{ minWidth: 0 }}>
              <Typography fontWeight={600}>
                <UserIdentity user={r} /><ProfileDetails user={r} />
              </Typography>
              <Typography
                variant="caption"
                color="text.secondary"
              >
                {moscowDate(r.created_at)}
              </Typography>
            </Box>
            {actions(r)}
          </Stack>
          <Typography variant="body2">
            {groups.find(g => g.id === r.requested_group_id)?.name || uiText("Группа не указана")}
          </Typography>
          <Typography
            variant="body2"
            sx={{ mt: 1, overflowWrap: "anywhere" }}
          >
            {r.purpose}
          </Typography>
          {r.status !== "pending" && <Typography
            variant="body2"
            color="text.secondary"
          >{requestLabels[r.status]} · {r.decided_username || "—"} · {moscowDate(r.decided_at)}</Typography>}
        </Box>
      )}
    </Stack> : <TableContainer>
      <Table
        size="small"
        className="admin-table"
      >
        <TableHead>
          <TableRow>
            {[
              uiText("Подана"),
              uiText("Пользователь"),
              uiText("Группа / назначение"),
              uiText("Статус"),
              uiText("Кто и когда решил"),
              uiText("Решение")
            ].map(h => <TableCell key={h}>
              {h}
            </TableCell>)}
          </TableRow>
        </TableHead>
        <TableBody>
          {visible.map(
            r => <TableRow
              key={r.id}
              hover
            >
              <TableCell>
                {moscowDate(r.created_at)}
              </TableCell>
              <TableCell>
                <UserIdentity user={r} /><ProfileDetails user={r} />
              </TableCell>
              <TableCell sx={{ maxWidth: 340 }}>
                <Typography
                  variant="body2"
                  fontWeight={600}
                >
                  {groups.find(g => g.id === r.requested_group_id)?.name || uiText("Не указана")}
                </Typography>
                <Typography
                  variant="body2"
                  color="text.secondary"
                >
                  {r.purpose}
                </Typography>
              </TableCell>
              <TableCell>
                <Chip
                  size="small"
                  variant="outlined"
                  label={requestLabels[r.status]}
                  color={r.status === "pending" ? "warning" : "default"}
                />
              </TableCell>
              <TableCell>
                {r.decided_username || "—"}
                <Typography
                  variant="caption"
                  display="block"
                >
                  {moscowDate(r.decided_at)}
                </Typography>
              </TableCell>
              <TableCell>
                {actions(r)}
              </TableCell>
            </TableRow>
          )}
        </TableBody>
      </Table>
    </TableContainer>}
    {!visible.length && <Empty
      text={tab === 0 ? uiText("Новых заявок нет. Здесь появятся запросы на доступ.") : uiText("Обработанных заявок пока нет.")}
    />}
    {!compact && <Pager
      total={filtered.length}
      page={page}
      size={size}
      setPage={setPage}
      setSize={setSize}
    />}
    <Dialog
      open={!!edit}
      onClose={() => !saving && setEdit(null)}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle>{approve ? uiText("Одобрить") : uiText("Отклонить")}{" " + uiText("заявку") + " "}{edit?.username}</DialogTitle>
      <DialogContent>
        <ProfileDetails user={edit} />
        <Stack
          gap={2}
          sx={{ pt: 1 }}
        >
          <Typography>
            {edit?.purpose}
          </Typography>
          {approve && <TextField
            select
            label={uiText("Группа доступа")}
            value={groupId}
            onChange={e => setGroupId(Number(e.target.value))}
          >
            {groups.filter(g => g.is_active).map(
              g => <MenuItem
                key={g.id}
                value={g.id}
              >
                {g.name}
              </MenuItem>
            )}
          </TextField>}
          <TextField
            label={approve ? uiText("Комментарий (необязательно)") : uiText("Причина отказа")}
            required={!approve}
            multiline
            value={comment}
            onChange={e => setComment(e.target.value)}
          />
          <Failure error={error} />
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={() => setEdit(null)}>{uiText("Отмена")}</Button>
        <Button
          variant="contained"
          color={approve ? "primary" : "error"}
          disabled={saving || (approve ? !groupId : !comment.trim())}
          onClick={async () => {
            setSaving(true);
            try {
              await accessCall(
                `/group-admin/requests/${edit?.id}/decision`,
                "POST",
                {
                  status: approve ? "approved" : "rejected",
                  ...(approve ? { group_id: groupId } : {}),
                  decision_comment: comment,
                }
              );
              setEdit(null);
              changed();
            } catch (e) { setError((e as Error).message); } finally { setSaving(false); }
          }}
        >
          {approve ? uiText("Одобрить") : uiText("Отклонить")}
        </Button>
      </DialogActions>
    </Dialog>
  </>;
  return compact ? content : <Panel title={uiText("Заявки на доступ")}>
    {content}
  </Panel>;
}
