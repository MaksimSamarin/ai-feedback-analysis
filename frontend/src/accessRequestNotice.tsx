import { uiText } from "./i18n";
import { useEffect, useState } from "react";
import {
  Alert,
  Box,
  Button,
  Chip,
  Stack,
  Typography,
  Tooltip
} from "@mui/material";
import { accessCall, type AccessState } from "./access";
import { requestNotice } from "./requestPresentation";
import { browserNoticeStorage, createRequestNoticeTracker } from "./requestNoticeState";
export function RequestAccessButton({ pending, onClick }: {
  pending: boolean;
  onClick: () => void
}) {
  return <Tooltip title={pending ? uiText("Уже есть заявка на рассмотрении") : ""}>
    <span style={{ display: "inline-flex", maxWidth: "100%" }}>
      <Button
        color="secondary"
        variant="outlined"
        disabled={pending}
        sx={{ minHeight: 40 }}
        onClick={onClick}
      >{uiText("Запросить доступ в другую группу")}</Button>
    </span>
  </Tooltip>;
}
export function AccessRequestNotice({ state, onChanged }: {
  state: AccessState;
  onChanged: (state: AccessState) => void
}) {
  const [dismissed, setDismissed] = useState<number | null>(null);
  const [tracker] = useState(() => createRequestNoticeTracker(browserNoticeStorage()));
  const [busy, setBusy] = useState(false), [error, setError] = useState("");
  const request = state.requests[0];
  const saved = tracker.state(request);
  const notice = requestNotice(request, state.groups, saved.dismissed ? request!.id : dismissed, saved.approved);
  useEffect(() => { tracker.observe(request); }, [tracker, request?.id, request?.status]);
  function dismiss() {
    if (!request) return;
    tracker.dismiss(request.id);
    setDismissed(request.id);
  }
  async function cancel() {
    if (!request) return;
    setBusy(true);
    setError("");
    try {
      await accessCall(`/access/requests/${request.id}/cancel`, "POST");
      onChanged(await accessCall<AccessState>("/access/status"));
    } catch (e) { setError((e as Error).message); } finally { setBusy(false); }
  }
  if (!notice && !error) return null;
  return <Box
    sx={{ minWidth: 0, maxWidth: "100%" }}
    aria-live="polite"
  >
    {notice?.kind === "pending" && <Stack
      direction="row"
      gap={.5}
      alignItems="center"
      flexWrap="wrap"
    >
      <Chip
        size="small"
        variant="outlined"
        label={notice.text}
        sx={{
          maxWidth: "100%",
          height: "auto",
          "& .MuiChip-label": { py: .5, whiteSpace: "normal", overflowWrap: "anywhere" }
        }}
      />
      <Button
        color="secondary"
        size="small"
        disabled={busy}
        onClick={() => void cancel()}
      >{uiText("Отменить")}</Button>
    </Stack>}
    {(notice?.kind === "rejected" || notice?.kind === "approved") && <Alert
      severity={notice.kind === "approved" ? "success" : "warning"}
      onClose={dismiss}
      closeText={uiText("Закрыть уведомление о решении заявки")}
      sx={{ overflowWrap: "anywhere" }}
    >
      {notice.text}
      {notice.comment && <Typography
        variant="body2"
        sx={{ whiteSpace: "pre-line" }}
      >
        {notice.comment}
      </Typography>}
    </Alert>}
    {error && <Alert
      severity="error"
      sx={{ mt: 1 }}
    >
      {error}
    </Alert>}
  </Box>;
}
