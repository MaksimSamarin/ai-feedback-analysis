import React from "react";
import ReactDOM from "react-dom/client";
import { api } from "./api";
import { setUiLanguage } from "./i18n";
import { AppThemeProvider } from "./theme";

async function start() {
  const settings = await api.getUiSettings().catch(() => ({ language: "en" as const }));
  setUiLanguage(settings.language);
  const { default: App } = await import("./App");

  ReactDOM.createRoot(document.getElementById("root")!).render(
    <React.StrictMode>
      <AppThemeProvider>
        <App />
      </AppThemeProvider>
    </React.StrictMode>,
  );
}

void start();
