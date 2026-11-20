import { uiText } from "./i18n";
import { useState, type ReactNode } from "react";
import {
  Box,
  Button,
  Drawer,
  IconButton,
  Stack,
  Typography,
  useMediaQuery
} from "@mui/material";
import CloseIcon from "@mui/icons-material/Close";
import FilterListIcon from "@mui/icons-material/FilterList";
export function AdminFilters({ search, children, count, before, after, onReset, columns = 3 }: {
  search: ReactNode;
  children: ReactNode;
  count: number;
  before?: ReactNode;
  after?: ReactNode;
  onReset: () => void;
  columns?: number;
}) {
  const mobile = useMediaQuery("(max-width:899px)");
  const [open, setOpen] = useState(false);
  if (!mobile) return <>
    {before}
    <Box
      className="admin-filter-row admin-filter-grid"
      sx={{ gridTemplateColumns: `repeat(${columns}, minmax(0, 1fr))` }}
    >
      {search}
      {children}
    </Box>
    {after}
  </>;
  return <>
    <Box className="admin-mobile-filterbar">
      <Box sx={{ flex: 1, minWidth: 0 }}>
        {search}
      </Box>
      <Button
        color="inherit"
        variant="outlined"
        startIcon={<FilterListIcon />}
        onClick={() => setOpen(true)}
      >{uiText("Фильтры (")}{count})
      </Button>
    </Box>
    <Drawer
      anchor="right"
      open={open}
      onClose={() => setOpen(false)}
      PaperProps={{ className: "admin-page admin-filter-drawer", sx: { width: 360, maxWidth: "100%" } }}
    >
      <Stack
        direction="row"
        alignItems="center"
        justifyContent="space-between"
        sx={{ p: 2 }}
      >
        <Typography
          component="h2"
          variant="h6"
        >{uiText("Фильтры")}</Typography>
        <IconButton
          aria-label={uiText("Закрыть фильтры")}
          onClick={() => setOpen(false)}
        >
          <CloseIcon />
        </IconButton>
      </Stack>
      <Box className="admin-filter-drawer-body">
        {before}
        <Stack gap={2}>
          {children}
        </Stack>
        {after}
      </Box>
      <Stack
        direction="row"
        justifyContent="space-between"
        gap={1}
        sx={{ p: 2 }}
      >
        <Button
          color="inherit"
          onClick={onReset}
        >{uiText("Сбросить")}</Button>
        <Button
          variant="contained"
          onClick={() => setOpen(false)}
        >{uiText("Готово")}</Button>
      </Stack>
    </Drawer>
  </>;
}
