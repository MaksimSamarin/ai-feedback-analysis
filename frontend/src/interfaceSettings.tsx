import { useState, type FormEvent } from "react";
import { Alert, Button, FormControl, InputLabel, MenuItem, Select, Stack, Typography } from "@mui/material";
import { api } from "./api";
import { Panel } from "./adminShared";
import { getUiLanguage, uiText, type UILanguage } from "./i18n";

export function InterfaceSettings() {
  const [language, setLanguage] = useState<UILanguage>(getUiLanguage);
  const [saving, setSaving] = useState(false);
  const [error, setError] = useState("");

  async function save(event: FormEvent) {
    event.preventDefault();
    setSaving(true);
    setError("");
    try {
      await api.saveUiSettings(language);
      window.location.reload();
    } catch {
      setError(uiText("Не удалось сохранить язык."));
      setSaving(false);
    }
  }

  return (
    <Panel title={uiText("Настройки")}>
      <Stack component="form" onSubmit={save} spacing={2} sx={{ alignItems: "flex-start", maxWidth: 480 }}>
        <FormControl fullWidth>
          <InputLabel id="interface-language-label">{uiText("Язык интерфейса")}</InputLabel>
          <Select
            labelId="interface-language-label"
            label={uiText("Язык интерфейса")}
            value={language}
            onChange={(event) => setLanguage(event.target.value as UILanguage)}
            disabled={saving}
          >
            <MenuItem value="en">English</MenuItem>
            <MenuItem value="ru">Русский</MenuItem>
          </Select>
        </FormControl>
        <Typography variant="body2" color="text.secondary">
          {uiText("Язык применяется ко всему интерфейсу, включая экран входа.")}
        </Typography>
        {error && <Alert severity="error">{error}</Alert>}
        <Button type="submit" variant="contained" disabled={saving || language === getUiLanguage()}>
          {saving ? uiText("Сохраняем…") : uiText("Сохранить")}
        </Button>
      </Stack>
    </Panel>
  );
}
