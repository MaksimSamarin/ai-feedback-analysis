import { uiText } from "./i18n";
import { UserIdentity, type Profile } from "./userIdentity";
import { useMediaQuery } from "@mui/material";
import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Checkbox,
  Chip,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  FormControlLabel,
  IconButton,
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
  Typography
} from "@mui/material";
import SettingsIcon from "@mui/icons-material/Settings";
import EditIcon from "@mui/icons-material/Edit";
import PersonRemoveIcon from "@mui/icons-material/PersonRemove";
import AddIcon from "@mui/icons-material/Add";
import { accessCall, type AccessGroup } from "./access";
import { quantity, moscowDate, UsageBar, Loading } from "./adminShared";
type Member = Profile & {
  role?: string;
  user_id: number;
  username: string;
  role_in_group: string;
  limit_mode: string;
  personal_token_limit: number | null;
  used: number
};
export function Groups({ groups, isAdmin, userId, changed }: {
  groups: AccessGroup[];
  isAdmin: boolean;
  userId: number;
  changed: () => void;
}) {
  const mobile = useMediaQuery("(max-width:899px)");
  const [selected, setSelected] = useState<number | null>(null);
  const [query, setQuery] = useState("");
  const [filter, setFilter] = useState("");
  const [members, setMembers] = useState<Member[]>([]);
  const [loading, setLoading] = useState(false);
  const [error, setError] = useState("");
  const [groupEdit, setGroupEdit] = useState<Partial<AccessGroup> | null>(null);
  const [memberEdit, setMemberEdit] = useState<Member | null>(null);
  const [remove, setRemove] = useState<Member | null>(null);
  const [add, setAdd] = useState(false);
  const [login, setLogin] = useState("");
  const [saving, setSaving] = useState(false);
  const group = groups.find(g => g.id === selected) || groups[0];
  async function load() {
    if (!group) {
      setMembers([]);
      return;
    }
    setLoading(true);
    try { setMembers(await accessCall<Member[]>(`/group-admin/groups/${group.id}/members`)); }
    catch (e) { setError((e as Error).message); } finally { setLoading(false); }
  }
  useEffect(() => { void load(); }, [group?.id, groups]);
  async function save(work: () => Promise<unknown>, done: () => void) {
    setSaving(true);
    setError("");
    try {
      await work();
      done();
      changed();
      await load();
    }
    catch (e) { setError((e as Error).message); } finally { setSaving(false); }
  }
  const visible = groups.filter(g => g.name.toLowerCase().includes(query.toLowerCase()));
  const filtered = members.filter(m => (m.username+" "+(m.full_name || "")).toLowerCase().includes(filter.toLowerCase()));
  return <>
    {error && <Alert
      severity="error"
      sx={{ mb: 2 }}
    >
      {error}
    </Alert>}
    <Box className="admin-groups">
      <Card className="admin-panel admin-group-list">
        <CardContent>
          <Stack
            direction="row"
            alignItems="center"
            justifyContent="space-between"
            sx={{ mb: 2 }}
          >
            <Typography variant="h6">{uiText("Группы ·") + " "}{groups.length}</Typography>
            {isAdmin && <Tooltip title={uiText("Создать группу")}>
              <IconButton
                aria-label={uiText("Создать группу")}
                onClick={() => setGroupEdit(
                  {
                    name: "",
                    description: "",
                    token_limit: null,
                    limit_period: "month",
                    is_active: true,
                  }
                )}
              >
                <AddIcon />
              </IconButton>
            </Tooltip>}
          </Stack>
          {mobile ? <TextField
            select
            fullWidth
            label={uiText("Выбрать группу")}
            value={group?.id || ""}
            onChange={e => {
              setSelected(Number(e.target.value));
              setFilter("");
              setError("");
            }}
          >
            {groups.map(
              g => <MenuItem
                key={g.id}
                value={g.id}
              >
                {g.name}
                {!g.is_active && " " + uiText("· отключена")}
              </MenuItem>
            )}
          </TextField> : <>
            {"          "}
            <TextField
              fullWidth
              size="small"
              label={uiText("Поиск группы")}
              value={query}
              onChange={e => setQuery(e.target.value)}
            />
            <Stack
              gap={1}
              sx={{ mt: 2 }}
            >
              {visible.map(
                g => <Button
                  key={g.id}
                  variant={g.id === group?.id ? "outlined" : "text"}
                  onClick={() => {
                    setSelected(g.id);
                    setFilter("");
                    setError("");
                  }}
                  sx={{ p: 1.5, textAlign: "left", opacity: g.is_active ? 1 : .55, display: "block" }}
                >
                  <Typography
                    color="text.primary"
                    fontWeight={600}
                  >
                    {g.name}
                  </Typography>
                  <Typography
                    variant="caption"
                    color="text.secondary"
                  >
                    {quantity(
                      (g as AccessGroup & {
                        member_count?: number;
                      }).member_count
                    )}{" " + uiText("участников")}{!g.is_active && " " + uiText("· отключена")}
                  </Typography>
                  <UsageBar
                    used={g.used}
                    limit={g.token_limit}
                    summary
                  />
                </Button>
              )}
              {!visible.length && <Typography color="text.secondary">{uiText("Группы не найдены")}</Typography>}
            </Stack>
          </>}
        </CardContent>
      </Card>
      <Card className="admin-panel">
        <CardContent>
          <Stack
            direction="row"
            gap={1}
            alignItems="center"
            justifyContent="space-between"
            flexWrap="wrap"
          >
            <Typography
              variant="h5"
              sx={{ overflowWrap: "anywhere", flex: "1 1 160px" }}
            >
              {group?.name || uiText("Нет групп")}
            </Typography>
            {isAdmin && <Stack
              direction="row"
              gap={1}
              sx={{ flexShrink: 0 }}
            >
              {group && <Tooltip title={uiText("Настройки группы")}>
                <IconButton
                  aria-label={uiText("Настройки группы")}
                  onClick={() => setGroupEdit({ ...group })}
                >
                  <SettingsIcon />
                </IconButton>
              </Tooltip>}
            </Stack>}
          </Stack>
          {group && <>
            <Typography
              variant="body2"
              color="text.secondary"
              sx={{ mt: 1 }}
            >
              {group.description || uiText("Описание не задано")}
            </Typography>
            <Stack
              direction="row"
              gap={1}
              flexWrap="wrap"
              sx={{ mt: 1.5 }}
            >
              <Chip
                size="small"
                variant="outlined"
                label={group.is_active ? uiText("Активна") : uiText("Отключена")}
              />
              <Chip
                size="small"
                variant="outlined"
                label={group.limit_period === "month" ? uiText("Лимит на месяц") : uiText("Лимит за всё время")}
              />
              <Chip
                size="small"
                variant="outlined"
                label={uiText("{0} участников", members.length)}
              />
            </Stack>
            <Box sx={{ my: 3 }}>
              <UsageBar
                used={group.used}
                limit={group.token_limit}
              />
              <Stack
                direction="row"
                justifyContent="space-between"
                gap={1}
                flexWrap="wrap"
                sx={{ mt: 1 }}
              >
                <Typography variant="caption">{uiText("Прошлый период:") + " "}{quantity(group.previous_used)}</Typography>
                <Typography variant="caption">
                  {group.reset_at ? uiText("Сброс: {0}", moscowDate(group.reset_at).replace(/\.\d{4}/, "")) : uiText("Без сброса")}
                </Typography>
              </Stack>
            </Box>
            <Stack
              direction="row"
              gap={1}
              flexWrap="wrap"
              justifyContent="space-between"
              sx={{ mb: 2 }}
            >
              <TextField
                size="small"
                label={uiText("Поиск участника")}
                value={filter}
                onChange={e => setFilter(e.target.value)}
              />
              <Button
                variant="outlined"
                startIcon={<AddIcon />}
                onClick={() => {
                  setLogin("");
                  setAdd(true);
                }}
              >{uiText("Добавить участника")}</Button>
            </Stack>
            {loading ? <Loading /> : <TableContainer>
              <Table
                size="small"
                sx={{ minWidth: 550 }}
              >
                <TableHead>
                  <TableRow>
                    <TableCell>{uiText("Участник")}</TableCell>
                    <TableCell>{uiText("Роль")}</TableCell>
                    <TableCell>{uiText("Лимит")}</TableCell>
                    <TableCell align="right">{uiText("Расход")}</TableCell>
                    <TableCell align="right">{uiText("Действия")}</TableCell>
                  </TableRow>
                </TableHead>
                <TableBody>
                  {filtered.map(
                    m => {
                      const editable = isAdmin || (m.user_id !== userId && m.role_in_group !== "manager");
                      return <TableRow
                        key={m.user_id}
                        hover
                      >
                        <TableCell sx={{ fontWeight: 600 }}>
                          <UserIdentity user={m} />
                        </TableCell>
                        <TableCell>
                          {m.role === "admin" ? uiText("Администратор") : m.role_in_group === "manager" ? uiText("Руководитель") : uiText("Пользователь")}
                        </TableCell>
                        <TableCell>
                          {m.limit_mode === "personal" ? quantity(m.personal_token_limit) : uiText("Групповой")}
                        </TableCell>
                        <TableCell align="right">
                          {quantity(m.used)}
                        </TableCell>
                        <TableCell
                          align="right"
                          sx={{ whiteSpace: "nowrap" }}
                        >
                          {editable && <>
                            <Tooltip title={uiText("Изменить лимит")}>
                              <IconButton
                                size="small"
                                aria-label={uiText("Изменить $<UserIdentity user={m} />")}
                                onClick={() => setMemberEdit({ ...m })}
                              >
                                <EditIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                            <Tooltip title={uiText("Исключить из группы")}>
                              <IconButton
                                color="error"
                                size="small"
                                aria-label={uiText("Исключить $<UserIdentity user={m} />")}
                                onClick={() => setRemove(m)}
                              >
                                <PersonRemoveIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          </>}
                        </TableCell>
                      </TableRow>;
                    }
                  )}
                  {!filtered.length && <TableRow>
                    <TableCell colSpan={5}>{uiText("Участников не найдено")}</TableCell>
                  </TableRow>}
                </TableBody>
              </Table>
            </TableContainer>}
          </>}
        </CardContent>
      </Card>
    </Box>
    <Dialog
      open={!!groupEdit}
      onClose={() => !saving && setGroupEdit(null)}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle>
        {groupEdit?.id ? uiText("Настройки группы") : uiText("Создать группу")}
      </DialogTitle>
      <DialogContent>
        <Stack
          gap={2}
          sx={{ pt: 1 }}
        >
          <TextField
            label={uiText("Название группы")}
            value={groupEdit?.name || ""}
            onChange={e => setGroupEdit(g => ({ ...g, name: e.target.value }))}
          />
          <TextField
            label={uiText("Описание")}
            multiline
            value={groupEdit?.description || ""}
            onChange={e => setGroupEdit(g => ({ ...g, description: e.target.value }))}
          />
          <TextField
            label={uiText("Лимит токенов")}
            type="number"
            helperText={uiText("Пустое поле — без лимита")}
            value={groupEdit?.token_limit ?? ""}
            onChange={e => setGroupEdit(g => ({ ...g, token_limit: e.target.value === "" ? null : Number(e.target.value) }))}
          />
          <TextField
            select
            label={uiText("Период")}
            value={groupEdit?.limit_period || "month"}
            onChange={e => setGroupEdit(g => ({ ...g, limit_period: e.target.value }))}
          >
            <MenuItem value="month">{uiText("Месяц по Москве")}</MenuItem>
            <MenuItem value="none">{uiText("За всё время")}</MenuItem>
          </TextField>
          <FormControlLabel
            label={uiText("Группа активна")}
            control={<Checkbox
              checked={groupEdit?.is_active ?? true}
              onChange={e => setGroupEdit(g => ({ ...g, is_active: e.target.checked }))}
            />}
          />
          {error && <Alert severity="error">
            {error}
          </Alert>}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button
          disabled={saving}
          onClick={() => setGroupEdit(null)}
        >{uiText("Отмена")}</Button>
        <Button
          variant="contained"
          disabled={saving || !groupEdit?.name?.trim()}
          onClick={() => void save(
            async () => {
              const saved = await accessCall<AccessGroup>(
                `/group-admin/groups${groupEdit?.id ? `/${groupEdit.id}` : ""}`,
                groupEdit?.id ? "PATCH" : "POST",
                groupEdit
              );
              setSelected(saved.id);
              return saved;
            },
            () => setGroupEdit(null),
          )}
        >{uiText("Сохранить")}</Button>
      </DialogActions>
    </Dialog>
    <Dialog
      open={add}
      onClose={() => !saving && setAdd(false)}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle>{uiText("Добавить участника в") + " "}{group?.name}</DialogTitle>
      <DialogContent>
        <TextField
          fullWidth
          label={uiText("Точный логин")}
          value={login}
          sx={{ mt: 1 }}
          helperText={uiText("Пользователь должен хотя бы один раз войти в сервис")}
          onChange={e => setLogin(e.target.value)}
        />
        {error && <Alert severity="error">
          {error}
        </Alert>}
      </DialogContent>
      <DialogActions>
        <Button onClick={() => setAdd(false)}>{uiText("Отмена")}</Button>
        <Button
          variant="contained"
          disabled={saving || !login.trim()}
          onClick={() => void save(
            () => accessCall(`/group-admin/groups/${group?.id}/members/by-login`, "POST", { username: login }),
            () => setAdd(false),
          )}
        >{uiText("Добавить")}</Button>
      </DialogActions>
    </Dialog>
    <Dialog
      open={!!memberEdit}
      onClose={() => !saving && setMemberEdit(null)}
      fullWidth
      maxWidth="sm"
    >
      <DialogTitle>{uiText("Лимит участника") + " "}{memberEdit?.username}</DialogTitle>
      <DialogContent>
        <Stack
          gap={2}
          sx={{ pt: 1 }}
        >
          <TextField
            select
            label={uiText("Режим лимита")}
            value={memberEdit?.limit_mode || "group"}
            onChange={e => setMemberEdit(m => m && ({ ...m, limit_mode: e.target.value }))}
          >
            <MenuItem value="group">{uiText("В пределах общего лимита группы")}</MenuItem>
            <MenuItem value="personal">{uiText("Личный лимит")}</MenuItem>
          </TextField>
          {memberEdit?.limit_mode === "personal" && <TextField
            label={uiText("Личный лимит")}
            type="number"
            value={memberEdit.personal_token_limit ?? ""}
            onChange={e => setMemberEdit(
              m => m && ({
                ...m,
                personal_token_limit: e.target.value === "" ? null : Number(e.target.value),
              })
            )}
          />}
          {isAdmin && <Alert severity="info">{uiText("Роль руководителя назначается в разделе «Пользователи».")}</Alert>}
          {error && <Alert severity="error">
            {error}
          </Alert>}
        </Stack>
      </DialogContent>
      <DialogActions>
        <Button onClick={() => setMemberEdit(null)}>{uiText("Отмена")}</Button>
        <Button
          variant="contained"
          disabled={saving}
          onClick={() => void save(
            () => accessCall(`/group-admin/groups/${group?.id}/members/${memberEdit?.user_id}`, "PUT", memberEdit),
            () => setMemberEdit(null),
          )}
        >{uiText("Сохранить")}</Button>
      </DialogActions>
    </Dialog>
    <Dialog
      open={!!remove}
      onClose={() => setRemove(null)}
    >
      <DialogTitle>{uiText("Исключить") + " "}{remove?.username}?</DialogTitle>
      <DialogContent>{uiText("Доступ к группе «")}{group?.name}{uiText("» будет закрыт. Учтённый расход сохранится.")}</DialogContent>
      <DialogActions>
        <Button onClick={() => setRemove(null)}>{uiText("Отмена")}</Button>
        <Button
          color="error"
          variant="contained"
          disabled={saving}
          onClick={() => void save(
            () => accessCall(`/group-admin/groups/${group?.id}/members/${remove?.user_id}`, "DELETE"),
            () => setRemove(null),
          )}
        >{uiText("Исключить")}</Button>
      </DialogActions>
    </Dialog>
  </>;
}
