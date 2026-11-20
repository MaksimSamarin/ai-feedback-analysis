import { uiGroupName, uiText } from "./i18n";
import { Typography } from "@mui/material";
import type { ReportItem } from "./types";
export function ReportQuota({ report }: {
  report: ReportItem | null | undefined
}) {
  if (!report) return null;
  const note = report.access_group_name && (report.access_group_active === false ? " " + uiText("· группа отключена")
    : report.access_group_member === false ? " " + uiText("· нет доступа") : "");
  return <Typography
    variant="caption"
    color="text.secondary"
    display="block"
    sx={{ overflowWrap: "anywhere" }}
  >{uiText("Квота:") + " "}{report.access_group_name ? uiGroupName(report.access_group_name) : uiText("не указана")}{note}
  </Typography>;
}
