import { uiText } from "./i18n";
import { Alert } from "@mui/material";
export function PartialDownloadHint() {
  return <Alert severity="info">{uiText("Будет выгружен срез на текущий момент; повторное скачивание без новых результатов — мгновенно")}</Alert>;
}
