import { ruRU } from "@mui/material/locale";
import { getUiLanguage } from "./i18n";
import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { CssBaseline, ThemeProvider, createTheme } from "@mui/material";
const THEME_MODE_KEY = "review_analyzer_theme_preference";
type Mode = "light" | "dark";
type ThemeModeContextValue = {
  mode: Mode;
  toggle: () => void;
};
const ThemeModeContext = createContext<ThemeModeContextValue>({
  mode: "light",
  toggle: () => undefined,
});
export function useThemeMode(): ThemeModeContextValue {
  return useContext(ThemeModeContext);
}
export function AppThemeProvider({ children }: {
  children: React.ReactNode;
}) {
  const [mode, setMode] = useState<Mode>(
    () => {
      const saved = localStorage.getItem(THEME_MODE_KEY);
      return saved === "dark" ? "dark" : "light";
    }
  );
  useEffect(
    () => {
      localStorage.setItem(THEME_MODE_KEY, mode);
      document.documentElement.setAttribute("data-theme", mode);
    },
    [mode]
  );
  const value = useMemo(
    () => ({
      mode,
      toggle: () => setMode((prev) => (prev === "light" ? "dark" : "light")),
    }),
    [mode]
  );
  const theme = useMemo(() => createAppTheme(mode), [mode]);
  return (
    <ThemeModeContext.Provider value={value}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        {children}
      </ThemeProvider>
    </ThemeModeContext.Provider>
  );
}
export function createAppTheme(mode: Mode) {
  return createTheme(
    {
      palette: {
        mode,
        primary: { main: mode === "light" ? "#465c82" : "#c2cfe8" },
        secondary: { main: mode === "light" ? "#526077" : "#bac5da" },
        background: {
          default: mode === "light" ? "#f7f8fb" : "#171c27",
          paper: mode === "light" ? "#ffffff" : "#202737",
        },
        text: {
          primary: mode === "light" ? "#202a3a" : "#edf1f8",
          secondary: mode === "light" ? "#596579" : "#b5c0d3",
        },
        success: { main: mode === "light" ? "#318060" : "#91c6ae" },
        warning: { main: mode === "light" ? "#ea580c" : "#fb923c" },  // orange 600/400 — без амбры-грязи
        error: { main: mode === "light" ? "#dc2626" : "#f87171" },
      },
      shape: { borderRadius: 14 },
      typography: {
        fontFamily: '"Segoe UI Variable", "Segoe UI", system-ui, sans-serif',
        h4: { fontWeight: 700, letterSpacing: "-0.025em" },
        h5: { fontWeight: 660, letterSpacing: "-0.02em" },
        h6: { fontWeight: 650, letterSpacing: "-0.015em" },
        subtitle1: { fontWeight: 600 },
        subtitle2: { fontWeight: 600 },
        button: { fontWeight: 620, letterSpacing: "-0.005em" },
      },
      components: {
        MuiCard: {
          styleOverrides: {
            root: {
              // Базовые свойства; .card в styles.css перекрывает важными.
              borderRadius: 16,
            },
          },
        },
        MuiButton: {
          defaultProps: {
            disableElevation: true,
          },
          styleOverrides: {
            root: {
              borderRadius: 10,
              textTransform: "none",
              fontWeight: 620,
              paddingInline: 16,
            },
            containedPrimary: ({ theme, ownerState }) => ({
              background: theme.palette.primary.main,
              color: mode === "light" ? "#ffffff" : "#202a3d",
              "&:hover": {
                background: mode === "light" ? "#364b71" : "#d3def1",
              },
              "&.Mui-disabled": {
                background: theme.palette.action.disabledBackground,
                backgroundImage: "none",
                color: theme.palette.action.disabled,
                cursor: "not-allowed",
                pointerEvents: ownerState.href || ('component' in ownerState && ownerState.component && ownerState.component !== 'button') ? "none" : "auto",
              },
            }),
            containedSuccess: ({ theme, ownerState }) => ({
              background: theme.palette.success.main,
              color: mode === "light" ? "#ffffff" : "#1d332c",
              "&:hover": { background: mode === "light" ? "#276b50" : "#a3d1bc" },
              "&.Mui-disabled": {
                background: theme.palette.action.disabledBackground,
                backgroundImage: "none",
                color: theme.palette.action.disabled,
                cursor: "not-allowed",
                pointerEvents: ownerState.href || ('component' in ownerState && ownerState.component && ownerState.component !== 'button') ? "none" : "auto",
              },
            }),
            outlined: {
              borderWidth: 1.5,
              "&:hover": {
                borderWidth: 1.5,
              },
            },
          },
        },
        MuiChip: {
          styleOverrides: {
            root: {
              fontWeight: 600,
              borderRadius: 10,
              letterSpacing: "-0.005em",
            },
          },
        },
        MuiTextField: {
          defaultProps: {
            size: "small",
          },
        },
        MuiSelect: {
          defaultProps: {
            size: "small",
          },
        },
        MuiOutlinedInput: {
          styleOverrides: {
            root: {
              borderRadius: 10,
            },
          },
        },
        MuiTableCell: {
          styleOverrides: {
            head: {
              fontWeight: 700,
              letterSpacing: "-0.005em",
              color: mode === "light" ? "#3a4359" : "#c0c8db",
              borderBottomColor: mode === "light"
                ? "rgba(15, 23, 42, 0.08)"
                : "rgba(255, 255, 255, 0.08)",
            },
            body: {
              borderBottomColor: mode === "light"
                ? "rgba(15, 23, 42, 0.06)"
                : "rgba(255, 255, 255, 0.06)",
            },
          },
        },
        MuiLinearProgress: {
          styleOverrides: {
            root: {
              height: 10,
              borderRadius: 999,
            },
            bar: {
              borderRadius: 999,
            },
          },
        },
        MuiTooltip: {
          defaultProps: {
            arrow: true,
          },
        },
        MuiPaper: {
          styleOverrides: {
            rounded: {
              borderRadius: 14,
            },
          },
        },
      },
    },
    getUiLanguage() === "ru" ? ruRU : {},
  );
}
