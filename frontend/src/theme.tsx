import { createContext, useContext, useEffect, useMemo, useState } from "react";
import { CssBaseline, ThemeProvider, createTheme } from "@mui/material";

const THEME_MODE_KEY = "review_analyzer_theme_mode";

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

export function AppThemeProvider({ children }: { children: React.ReactNode }) {
  const [mode, setMode] = useState<Mode>(() => {
    const saved = localStorage.getItem(THEME_MODE_KEY);
    return saved === "dark" ? "dark" : "light";
  });

  useEffect(() => {
    localStorage.setItem(THEME_MODE_KEY, mode);
    document.documentElement.setAttribute("data-theme", mode);
  }, [mode]);

  const value = useMemo(() => ({
    mode,
    toggle: () => setMode((prev) => (prev === "light" ? "dark" : "light")),
  }), [mode]);

  const theme = useMemo(() => createTheme({
    palette: {
      mode,
      primary: { main: mode === "light" ? "#1976d2" : "#90caf9" },
      secondary: { main: mode === "light" ? "#455a64" : "#b0bec5" },
      background: {
        default: mode === "light" ? "#f6f8fc" : "#0f1320",
        paper: mode === "light" ? "#ffffff" : "#171d2e",
      },
      success: { main: mode === "light" ? "#1f9d55" : "#4cd98a" },
      warning: { main: mode === "light" ? "#d98324" : "#f3b04a" },
      error: { main: mode === "light" ? "#d32f2f" : "#ff6b6b" },
    },
    shape: { borderRadius: 12 },
    typography: {
      fontFamily: '"Manrope", "IBM Plex Sans", "Segoe UI", sans-serif',
      h4: { fontWeight: 800, letterSpacing: "-0.02em" },
      h6: { fontWeight: 700, letterSpacing: "-0.01em" },
    },
    components: {
      MuiCard: {
        styleOverrides: {
          root: {
            border: mode === "light" ? "1px solid rgba(15, 23, 42, 0.08)" : "1px solid rgba(255, 255, 255, 0.08)",
            boxShadow: mode === "light" ? "0 10px 30px rgba(17, 24, 39, 0.08)" : "0 10px 30px rgba(0, 0, 0, 0.35)",
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
            fontWeight: 700,
          },
          containedPrimary: {
            background: mode === "light"
              ? "linear-gradient(135deg, #1976d2 0%, #0d47a1 100%)"
              : "linear-gradient(135deg, #64b5f6 0%, #1976d2 100%)",
          },
        },
      },
      MuiChip: {
        styleOverrides: {
          root: {
            fontWeight: 600,
            borderRadius: 10,
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
    },
  }), [mode]);

  return (
    <ThemeModeContext.Provider value={value}>
      <ThemeProvider theme={theme}>
        <CssBaseline />
        {children}
      </ThemeProvider>
    </ThemeModeContext.Provider>
  );
}
