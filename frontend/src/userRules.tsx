import { uiText } from "./i18n";
import { forwardRef, useEffect, useImperativeHandle, useRef, useState } from "react";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Box,
  Button,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Skeleton,
  Typography,
  useMediaQuery
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ReactMarkdown from "react-markdown";
import { api } from "./api";
export type RulesHandle = {
  reveal: () => void
};
export const markdownSx = {
  overflowWrap: "anywhere",
  "& h1, & h2, & h3": { fontWeight: 700, mt: 2, mb: 1 },
  "& h1": { fontSize: "1.3rem" },
  "& h2": { fontSize: "1.2rem" },
  "& h3": { fontSize: "1.05rem" },
  "& p": { my: 1, lineHeight: 1.6 },
  "& ul, & ol": { pl: 3, my: 1 },
  "& li": { mb: 1.25 },
  "& code": { bgcolor: "action.hover", px: .6, py: .1, borderRadius: .5, fontSize: ".88em" },
  "& strong": { fontWeight: 600 },
  "& hr": { my: 2, border: 0, borderTop: 1, borderColor: "divider" },
};
export function UserMarkdown({ content }: {
  content: string
}) {
  return <Box sx={markdownSx}>
    <ReactMarkdown>
      {content}
    </ReactMarkdown>
  </Box>;
}
function RulesContent() {
  const [text, setText] = useState<string | null>(null), [error, setError] = useState(false), [revision, setRevision] = useState(0);
  useEffect(
    () => {
      let active = true;
      setError(false);
      setText(null);
      void api.getUserRules().then(data => { if (active) setText(data.content_md); }).catch(() => { if (active) setError(true); });
      return () => { active = false; };
    },
    [revision]
  );
  if (error) return <Alert
    severity="warning"
    action={<Button
      color="inherit"
      onClick={() => setRevision(n => n + 1)}
    >{uiText("Повторить")}</Button>}
  >{uiText("Не удалось загрузить правила. Попробуйте ещё раз.")}</Alert>;
  if (text === null) return <Box
    aria-label={uiText("Загрузка правил")}
    aria-busy="true"
  >
    {[1, 2, 3, 4].map(n => <Skeleton
      key={n}
      height={36}
    />)}
  </Box>;
  return <UserMarkdown content={text} />;
}
export const UserRulesBlock = forwardRef<RulesHandle, {
  id: string;
  compact?: boolean
}>(
  function UserRulesBlock({ id, compact = false }, ref) {
    const mobile = useMediaQuery("(max-width:899px)");
    const [expanded, setExpanded] = useState(!compact);
    const element = useRef<HTMLDivElement>(null);
    useImperativeHandle(
      ref,
      () => ({
        reveal() {
          setExpanded(true);
          requestAnimationFrame(
            () => {
              element.current?.scrollIntoView(
                {
                  block: "start",
                  behavior: window.matchMedia("(prefers-reduced-motion: reduce)").matches ? "auto" : "smooth"
                }
              );
              element.current?.focus({ preventScroll: true });
            }
          );
        }
      })
    );
    return <Box
      ref={element}
      id={id}
      tabIndex={-1}
      sx={{ minWidth: 0, scrollMarginTop: 24, outlineOffset: 4 }}
    >
      {mobile || compact ? <Accordion
        expanded={expanded}
        onChange={(_, value) => setExpanded(value)}
        disableGutters
        elevation={0}
        sx={{ bgcolor: "transparent", boxShadow: "none", "&:before": { display: "none" } }}
      >
        <AccordionSummary
          expandIcon={<ExpandMoreIcon />}
          aria-controls={`${id}-content`}
          id={`${id}-heading`}
        >
          <Typography
            component="h2"
            variant="h6"
          >{uiText("Правила работы")}</Typography>
        </AccordionSummary>
        <AccordionDetails id={`${id}-content`}>
          <RulesContent />
        </AccordionDetails>
      </Accordion> : <Box
        component="section"
        aria-labelledby={`${id}-heading`}
      >
        <Typography
          id={`${id}-heading`}
          component="h2"
          variant="h6"
          sx={{ mb: 1 }}
        >{uiText("Правила работы")}</Typography>
        <RulesContent />
      </Box>}
    </Box>;
  }
);
export function UserRulesDialog({ open, onClose }: {
  open: boolean;
  onClose: () => void
}) {
  return <Dialog
    open={open}
    onClose={onClose}
    fullWidth
    maxWidth="md"
  >
    <DialogTitle>{uiText("Правила работы")}</DialogTitle>
    <DialogContent dividers>
      <RulesContent />
    </DialogContent>
    <DialogActions>
      <Button
        color="inherit"
        onClick={onClose}
      >{uiText("Закрыть")}</Button>
    </DialogActions>
  </Dialog>;
}
