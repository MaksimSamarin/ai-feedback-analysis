import { uiText } from "./i18n";
export type RequestNotice = {
  id: number;
  status: string;
  requested_group_id: number | null;
  requested_group_name?: string | null;
  decision_comment?: string;
};
export function requestGroupName(request: RequestNotice, groups: Array<{
  id: number;
  name: string
}>) {
  return request.requested_group_name || groups.find(g => g.id === request.requested_group_id)?.name
    || (request.requested_group_id ? uiText("группа № {0}", request.requested_group_id) : uiText("не выбрана"));
}
export function requestNotice(request: RequestNotice | undefined, groups: Array<{
  id: number;
  name: string
}>, dismissed: number | null, showApproved = false) {
  if (!request) return null;
  const name = requestGroupName(request, groups);
  if (request.status === "pending") return { kind: "pending" as const, text: uiText("Заявка в «{0}» на рассмотрении", name), comment: "" };
  if (request.status === "approved" && showApproved && request.id !== dismissed)
    return {
      kind: "approved" as const,
      text: uiText("Доступ в «{0}» одобрен — группа появилась в выборе квоты", name),
      comment: ""
    };
  if (request.status === "rejected" && request.id !== dismissed)
    return {
      kind: "rejected" as const,
      text: uiText("Заявка в «{0}» отклонена", name),
      comment: request.decision_comment || ""
    };
  return null;
}
export function requestInstructions(groups: Array<{
  name: string
}>, pending: boolean) {
  const access = groups.length ? uiText("Сейчас у вас доступ к: {0}.", groups.map(g => g.name).join(", ")) : "";
  return [access, pending ? "" : uiText("Выберите группу, в которую нужен доступ, и опишите задачу.")].filter(Boolean).join(" ");
}
