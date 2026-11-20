import { uiText } from "./i18n";
import { Box, Typography } from "@mui/material";
export type Profile = { full_name?: string | null; job_title?: string | null; company?: string | null; employee_id?: string | null };
export function UserIdentity({ user }: { user: Profile & { username?: string } }) {
  return <Box component="span" sx={{ display: "inline-block", overflowWrap: "anywhere" }}>
    {user.full_name || user.username || "—"}
    {user.full_name && <Typography component="span" display="block" variant="caption" color="text.secondary">{user.username}</Typography>}
  </Box>;
}
export function ProfileDetails({ user }: { user: Profile | null }) {
  return <Box component="span" sx={{ display: "block", mb: 2, overflowWrap: "anywhere" }}>
    {user?.job_title && <Typography component="span" display="block" variant="body2">{uiText("Должность:") + " "}{user.job_title}</Typography>}
    {user?.company && <Typography component="span" display="block" variant="body2">{uiText("Компания:") + " "}{user.company}</Typography>}
    {user?.employee_id && <Typography component="span" display="block" variant="body2">{uiText("Идентификатор сотрудника:") + " "}{user.employee_id}</Typography>}
  </Box>;
}
