import { uiText, uiLocale } from "./i18n";
import type { ReactNode } from "react";
import { useEffect, useId, useRef, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Card,
  CardContent,
  Checkbox,
  DialogActions,
  DialogContent,
  MenuItem,
  Stack,
  TextField,
  Typography,
  Link,
  useMediaQuery
} from "@mui/material";
import { UserRulesBlock, type RulesHandle } from "./userRules";
import { api } from "./api";
import { requestGroupName, requestInstructions } from "./requestPresentation";
import { purposeError } from "./requestValidation";
import { browserNoticeStorage, createRequestNoticeTracker } from "./requestNoticeState";
export type AccessGroup = {
  id: number;
  name: string;
  description: string;
  token_limit: number | null;
  limit_period: string;
  is_active: boolean;
  is_system: boolean;
  used: number;
  previous_used: number;
  reset_at: string | null;
  role_in_group?: string;
  limit_mode?: string;
  personal_token_limit?: number | null;
  personal_used?: number;
  group_remaining?: number | null;
  personal_remaining?: number | null;
  remaining: number | null;
};
export type AccessRequest = import("./userIdentity").Profile & {
  id: number;
  user_id: number;
  username?: string;
  purpose: string;
  status: string;
  requested_group_id: number | null;
  decision_comment?: string;
  requested_group_name?: string | null;
  created_at?: string;
};
export type AccessState = {
  has_access: boolean;
  role: string;
  groups: AccessGroup[];
  requests: AccessRequest[];
  last_group_id: number | null;
  default_group_id?: number | null;
  is_manager: boolean;
};
export async function accessCall<T>(path: string, method = "GET", data?: unknown): Promise<T> {
  const response = await fetch(
    `${api.base.endsWith("/api") ? api.base : api.base + "/api"}${path}`,
    {
      credentials: "include",
      method,
      headers: data === undefined ? {} : { "Content-Type": "application/json" },
      body: data === undefined ? undefined : JSON.stringify(data),
    }
  );
  const body = await response.json();
  if (!response.ok) throw new Error(
    typeof body.detail === "string" ? body.detail : body.detail?.message || JSON.stringify(body.detail)
  );
  return body as T;
}
export function RulesAcknowledgement({ id, checked, onChange, rulesId, showRules }: {
  id: string; checked: boolean; onChange: (checked: boolean) => void; rulesId: string; showRules: () => void;
}) {
  return <Box sx={{ display: "flex", alignItems: "center", gap: 1 }}>
    <Checkbox id={id} checked={checked} onChange={e => onChange(e.target.checked)} />
    <Box component="label" htmlFor={id} sx={{ cursor: "pointer", lineHeight: 1.5 }}>{uiText("Я прочитал(а)") + " "}<Link href={`#${rulesId}`} onClick={e => {
        e.preventDefault(); e.stopPropagation(); showRules();
      }}>{uiText("правила работы")}</Link>
    </Box>
  </Box>;
}
export function AccessPage({ state, logout, logoSrc, accountActions }: {
  state: AccessState;
  logout: () => void;
  logoSrc: string;
  accountActions?: ReactNode;
}) {
  const rules = useRef<RulesHandle>(null), rulesId = `access-rules-${useId()}`;
  const mobile = useMediaQuery("(max-width:899px)");
  const formCard = <Card key="form" className="card access-form-card">
    <CardContent>
      <AccessRequestForm
        state={state}
        rulesId={rulesId}
        onShowRules={() => rules.current?.reveal()}
        onChanged={next => { if (next.has_access) window.location.reload(); }}
      />
    </CardContent>
  </Card>;
  const rulesCard = <Card key="rules" className="card access-rules-card">
    <CardContent>
      <UserRulesBlock ref={rules} id={rulesId} />
    </CardContent>
  </Card>;
  return <Box className="page access-page">
    <Box className="container">
      <Box
        component="header"
        className="access-header"
      >
        <Box sx={{ display: "flex", alignItems: "center", gap: 1.5, minWidth: 0 }}>
          <img
            className="access-logo"
            src={logoSrc}
            alt={uiText("Логотип")}
          />
          <Typography
            variant="h6"
            sx={{ overflowWrap: "anywhere" }}
          >Analyzer</Typography>
        </Box>
        <Box sx={{ flexShrink: 0 }}>
          {accountActions || <Button
            color="secondary"
            onClick={logout}
          >{uiText("Выйти")}</Button>}
        </Box>
      </Box>
      <Box component="main">
        <Typography
          component="h1"
          variant="h4"
          sx={{ mb: 1 }}
        >{uiText("Запрос доступа")}</Typography>
        <Typography
          color="text.secondary"
          sx={{ mb: 3 }}
        >{uiText("Анализ отзывов из файлов. Заявку рассмотрит руководитель группы; доступ появится без повторного входа.")}</Typography>
        <Box className="access-layout">
          {mobile ? [rulesCard, formCard] : [formCard, rulesCard]}
        </Box>
      </Box>
    </Box>
  </Box>;
}
export function AccessRequestForm({ state, onChanged, secondary = false, onClose, rulesId, onShowRules }: {
  state: AccessState;
  onChanged: (state: AccessState) => void;
  secondary?: boolean;
  onClose?: () => void;
  rulesId?: string;
  onShowRules?: () => void;
}) {
  const ackId = `rules-ack-${useId()}`;
  const ownRules = useRef<RulesHandle>(null), ownRulesId = `request-rules-${useId()}`;
  const targetRulesId = rulesId || ownRulesId;
  const showRules = () => { if (onShowRules) onShowRules(); else ownRules.current?.reveal(); };
  const [current, setCurrent] = useState(state);
  const mounted = useRef(true);
  const [groups, setGroups] = useState<Array<{
    id: number;
    name: string;
    description?: string;
  }>>([]);
  const [group, setGroup] = useState("");
  const [catalogLoading, setCatalogLoading] = useState(true);
  const [purpose, setPurpose] = useState("");
  const [attempted, setAttempted] = useState(false);
  const [ack, setAck] = useState(false);
  const [busy, setBusy] = useState(false), [error, setError] = useState("");
  const pending = current.requests.find(r => r.status === "pending"), latest = current.requests[0];
  async function refresh() {
    const next = await accessCall<AccessState>("/access/status");
    if (!mounted.current) return;
    setCurrent(next);
    onChanged(next);
    const eligible = await accessCall<typeof groups>("/access/groups?for_request=true");
    if (!mounted.current) return;
    setGroups(eligible);
    setCatalogLoading(false);
    setGroup(value => eligible.some(g => String(g.id) === value) ? value : "");
  }
  useEffect(
    () => {
      mounted.current = true;
      void refresh().catch(e => { if (mounted.current) setError(e.message); });
      const timer = window.setInterval(() => void refresh().catch(e => { if (mounted.current) setError(e.message); }), 5000);
      return () => {
        mounted.current = false;
        clearInterval(timer);
      };
    },
    []
  );
  async function send(cancel = false) {
    setBusy(true);
    setError("");
    try {
      const changed = await accessCall<AccessRequest>(
        cancel ? `/access/requests/${pending?.id}/cancel` : "/access/requests",
        "POST",
        cancel ? undefined : { requested_group_id: group ? Number(group) : null, purpose, instructions_ack: ack }
      );
      if (!cancel) createRequestNoticeTracker(browserNoticeStorage()).observe(changed);
      await refresh();
      if (secondary && !cancel) onClose?.();
    } catch (e) { if (mounted.current) setError((e as Error).message); }
    finally { if (mounted.current) setBusy(false); }
  }
  const disabled = busy || !purpose.trim() || !ack || (secondary && !group);
  const body = <Stack
    spacing={2}
    sx={{ minWidth: 0 }}
  >
    {secondary && <Typography sx={{ overflowWrap: "anywhere" }}>
      {requestInstructions(current.groups, !!pending)}
    </Typography>}
    {!rulesId && <UserRulesBlock
      id={ownRulesId}
      ref={ownRules}
      compact
    />}
    {error && <Alert severity="error">
      {error}
    </Alert>}
    {latest && (pending || !secondary || latest.status === "rejected") && <Alert
      severity={latest.status === "rejected" ? "warning" : "info"}
      action={!secondary && pending ? <Button
        color="inherit"
        disabled={busy}
        onClick={() => void send(true)}
      >{uiText("Отменить")}</Button> : undefined}
      sx={{ overflowWrap: "anywhere", flexWrap: "wrap" }}
    >{uiText("Статус:") + " "}{({
        pending: uiText("Ожидает рассмотрения"),
        approved: uiText("Одобрена"),
        rejected: uiText("Отказано"),
        canceled: uiText("Отменена")
      } as Record<string, string>)[latest.status] || latest.status}
      {latest.created_at && <Typography variant="body2">{uiText("Подана:") + " "}{new Intl.DateTimeFormat(
        uiLocale(),
        {
          timeZone: "Europe/Moscow",
          day: "2-digit",
          month: "2-digit",
          year: "numeric",
          hour: "2-digit",
          minute: "2-digit"
        }
      ).format(new Date(latest.created_at))}{" " + uiText("МСК")}</Typography>}
      <Typography variant="body2">{uiText("Группа:") + " "}{requestGroupName(latest, [...current.groups, ...groups])}</Typography>
      {latest.decision_comment && <Typography>
        {latest.decision_comment}
      </Typography>}
    </Alert>}
    {!pending && <>
      <TextField
        select
        required={secondary}
        fullWidth
        label={uiText("Группа доступа")}
        disabled={busy || catalogLoading}
        helperText={catalogLoading ? uiText("Загрузка групп…") : undefined}
        value={group}
        SelectProps={{
          renderValue: value => groups.find(g => String(g.id) === value)?.name || uiText("Не знаю, какая группа")
        }}
        onChange={e => setGroup(e.target.value)}
      >
        <MenuItem
          value=""
          disabled={secondary}
        >
          {secondary ? uiText("Выберите группу") : <Box>
            <Typography variant="body2">{uiText("Не знаю, какая группа")}</Typography>
            <Typography
              variant="caption"
              color="text.secondary"
            >{uiText("Заявку рассмотрит администратор")}</Typography>
          </Box>}
        </MenuItem>
        {groups.map(
          g => <MenuItem
            key={g.id}
            value={String(g.id)}
            sx={{ whiteSpace: "normal", overflowWrap: "anywhere" }}
          >
            <Box sx={{ minWidth: 0 }}>
              <Typography variant="body2">
                {g.name}
              </Typography>
              {g.description && <Typography
                variant="caption"
                color="text.secondary"
                display="block"
              >
                {g.description}
              </Typography>}
            </Box>
          </MenuItem>
        )}
      </TextField>
      {secondary && !catalogLoading && !groups.length && <Alert severity="info">{uiText("Нет других активных групп для заявки.")}</Alert>}
      <TextField
        fullWidth
        multiline
        minRows={3}
        label={uiText("Для каких задач нужен инструмент")}
        InputLabelProps={{ required: true }}
        error={purposeError(purpose, attempted)}
        helperText={purposeError(purpose, attempted) ? uiText("Опишите, для каких задач нужен доступ.") : undefined}
        value={purpose}
        inputProps={{ maxLength: 4000, "aria-required": true }}
        onChange={e => setPurpose(e.target.value)}
      />
      <RulesAcknowledgement id={ackId} checked={ack} onChange={setAck} rulesId={targetRulesId} showRules={showRules} />
    </>}
    {pending && <Button
      color="secondary"
      size="small"
      disabled={busy}
      sx={{ alignSelf: "flex-start" }}
      onClick={() => void refresh().catch(e => setError(e.message))}
    >{uiText("Обновить статус")}</Button>}
  </Stack>;
  const sendButton = <Button
    type="submit"
    variant="contained"
    disabled={!!disabled}
  >{uiText("Отправить заявку")}</Button>;
  const cancelRequest = <Button
    color="secondary"
    disabled={busy}
    onClick={() => void send(true)}
  >{uiText("Отменить заявку")}</Button>;
  if (secondary) return <Box
    component="form"
    noValidate
    onSubmit={e => {
      e.preventDefault();
      setAttempted(true);
      if (!pending && !disabled) void send();
    }}
  >
    <DialogContent sx={{ pt: 1 }}>
      {body}
    </DialogContent>
    <DialogActions sx={{ px: 3, pb: 2, gap: 1 }}>
      <Button
        color="inherit"
        disabled={busy}
        onClick={onClose}
      >
        {pending ? uiText("Закрыть") : uiText("Отмена")}
      </Button>
      {pending ? cancelRequest : sendButton}
    </DialogActions>
  </Box>;
  return <Box
    component="form"
    noValidate
    onSubmit={e => {
      e.preventDefault();
      setAttempted(true);
      if (!pending && !disabled) void send();
    }}
  >
    {body}
    {!pending && <Stack
      direction="row"
      justifyContent="flex-end"
      sx={{ mt: 2 }}
    >
      {sendButton}
    </Stack>}
  </Box>;
}
export { GroupUsage } from "./quotaSection";
