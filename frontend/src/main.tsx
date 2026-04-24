import React from "react";
import ReactDOM from "react-dom/client";

import App from "./App";
import { AppThemeProvider } from "./theme";

ReactDOM.createRoot(document.getElementById("root")!).render(
  <React.StrictMode>
    <AppThemeProvider>
      <App />
    </AppThemeProvider>
  </React.StrictMode>,
);
