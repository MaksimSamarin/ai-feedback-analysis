import { getUiLanguage, uiText, uiLocale } from "./i18n";
import { englishReadyPreset } from "./englishPresets";
import { ModelSetupNotice, EMPTY_MODELS_TEXT } from "./modelSetup";
import { PartialDownloadHint } from "./downloadPresentation";
import { UserIdentity } from "./userIdentity";
import { chooseModel, restoreProvider, type ModelChoice } from "./modelChoice";
import { UserRulesDialog, UserMarkdown } from "./userRules";
import { shouldAutoShowRelease, releaseNotesErrorMessage } from "./releaseVisibility";
import { createFocusAccessRefresh } from "./accessRefresh";
import { rememberPendingRequests } from "./requestNoticeState";
import { AccessRequestNotice, RequestAccessButton } from "./accessRequestNotice";
import { revealLaunchElement } from "./launchFocus";
import { chooseQuota, quotaAvailable, quotaBlockedReason, retainQuotaSelection } from "./quotaChoice";
import { ReportQuota } from "./reportQuota";
import { useEffect, useId, useMemo, useRef, useState, type ClipboardEvent } from "react";
import {
  Accordion,
  AccordionDetails,
  AccordionSummary,
  Alert,
  Autocomplete,
  Box,
  Button,
  Card,
  CardContent,
  Drawer,
  Tabs,
  Tab,
  Checkbox,
  Chip,
  Collapse,
  Dialog,
  DialogActions,
  DialogContent,
  DialogTitle,
  Divider,
  FormControl,
  FormControlLabel,
  IconButton,
  InputLabel,
  LinearProgress,
  Link,
  ListItemIcon,
  ListItemText,
  Menu,
  MenuItem,
  Radio,
  RadioGroup,
  Select,
  Snackbar,
  Stack,
  Switch,
  Table,
  TableBody,
  TableCell,
  TableContainer,
  TableHead,
  TableRow,
  TextField,
  Tooltip,
  Typography,
} from "@mui/material";
import ExpandMoreIcon from "@mui/icons-material/ExpandMore";
import ExpandLessIcon from "@mui/icons-material/ExpandLess";
import CloudUploadIcon from "@mui/icons-material/CloudUpload";
import FolderOpenIcon from "@mui/icons-material/FolderOpen";
import CloseIcon from "@mui/icons-material/Close";
import NewReleasesIcon from "@mui/icons-material/NewReleases";
import MenuIcon from "@mui/icons-material/Menu";
import DescriptionIcon from "@mui/icons-material/Description";
import PlayArrowIcon from "@mui/icons-material/PlayArrow";
import StopIcon from "@mui/icons-material/Stop";
import PauseIcon from "@mui/icons-material/Pause";
import DownloadIcon from "@mui/icons-material/Download";
import SaveIcon from "@mui/icons-material/Save";
import RestartAltIcon from "@mui/icons-material/RestartAlt";
import LogoutIcon from "@mui/icons-material/Logout";
import DarkModeIcon from "@mui/icons-material/DarkMode";
import LightModeIcon from "@mui/icons-material/LightMode";
import DeleteOutlineIcon from "@mui/icons-material/DeleteOutline";
import AddIcon from "@mui/icons-material/Add";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import SettingsIcon from "@mui/icons-material/Settings";
import RefreshIcon from "@mui/icons-material/Refresh";
import { Pie, PieChart, ResponsiveContainer, Tooltip as RechartsTooltip } from "recharts";

import { api, APIError, type ExampleFile, type ReleaseEntry } from "./api";
import { GrabScrollBox } from "./useGrabScroll";
import type {
  AuthConfig,
  FileInspectResponse,
  JobSummary,
  Provider,
  ReportAnalysis,
  ReportItem,
  Usage,
  User,
  UserPreset,
  VerifyTokenResult,
} from "./types";
import "./styles.css";
import "./workspace.css";
import { useThemeMode } from "./theme";
import { AccessPage, AccessRequestForm, GroupUsage, accessCall, type AccessState, type AccessGroup } from "./access";

import { AdminScreen, navigate, useLocationHash } from "./admin";

const TOKEN_SESSION_KEY = "review_analyzer_provider_token";
const LAST_PROVIDER_KEY = "review_analyzer_last_provider";
const LAST_MODEL_KEY = "review_analyzer_last_model";
// При апдейте APP_VERSION диалог «Заметки о релизе» автоматически
// открывается один раз для каждого пользователя (см. useEffect ниже).
const RELEASE_NOTES_SEEN_VERSION_KEY = "review_analyzer_release_notes_seen_version";
const APP_VERSION = "2.2.0";

function formatQueueHint(position: number | null | undefined): string {
  if (typeof position !== "number" || position < 0) return "";
  if (position === 0) return uiText("Следующий в очереди");
  return uiText("Перед вами: {0}", position);
}

/**
 * Хинт под чипом «В очереди». Различает два случая:
 *   — personal_queue=true → отчёт ждёт пока юзер освободит свой slot
 *     (превышен MAX_ACTIVE_PER_USER на пользователя)
 *   — обычная queued → стандартная позиция в общей очереди.
 */
function formatQueuedHint(r: { personal_queue?: boolean; queue_position?: number | null }): string {
  if (r.personal_queue) {
    return uiText("Ждём пока завершится один из ваших активных отчётов и встанем в очередь");
  }
  return formatQueueHint(r.queue_position);
}
const BRAND_LOGO_CANDIDATES = [
  "/branding/logo.png",
  "/branding/logo.svg",
  "/review-pixel.png",
  "/branding/logo-default.png",
  "/logo-default.svg",
] as const;
type ReadyPresetId = "fraud_individual" | "fraud_by_store";
const READY_PRESET_IDS: ReadyPresetId[] = ["fraud_individual", "fraud_by_store"];
const READY_PRESETS: Array<{ id: ReadyPresetId; label: string }> = [
  { id: "fraud_individual", label: uiText("Пример: Проверка отзывов на мошенничество сотрудников") },
  { id: "fraud_by_store", label: uiText("Пример: Анализ мошенничества по магазинам (группировка)") },
];
const API_ROOT = api.base.endsWith("/api") ? api.base : `${api.base}/api`;

/** Число из поля ввода: принимает десятичную запятую («2,5»); пустое или некорректное — null. */
export function parseDecimal(raw: string): number | null {
  const text = raw.trim().replace(/\s+/g, "").replace(",", ".");
  if (!text) return null;
  const value = Number(text);
  return Number.isFinite(value) ? value : null;
}

type BuilderFieldType = "text" | "number" | "datetime" | "list";
type BuilderDateMode = "date" | "datetime";

type SchemaField = {
  id: string;
  name: string;
  type: BuilderFieldType;
  textMinLength: string;
  textMaxLength: string;
  numberMin: string;
  numberMax: string;
  numberIntegerOnly: boolean;
  datetimeMode: BuilderDateMode;
  listValues: string[];
  listSingle: boolean;
  listMinItems: string;
  listMaxItems: string;
};

function fmtDate(value: string | null): string {
  if (!value) return "-";
  const d = new Date(value);
  if (Number.isNaN(d.getTime())) return value;
  return d.toLocaleString(uiLocale());
}

/**
 * Рендерит содержимое MUI InputLabel: текст поля + иконка ⓘ с Tooltip подсказкой.
 * Используется внутри <InputLabel> / в `label` prop TextField, чтобы info-иконка
 * жила в notched outline поля (паттерн Material Design).
 *
 * По умолчанию у MUI InputLabel `pointer-events: none`, из-за чего Tooltip на
 * иконке не срабатывает — возвращаем `pointerEvents: auto` на span-обёртке.
 */
function FieldLabelContent({ label, hint }: { label: string; hint: string }) {
  return (
    <Stack component="span" direction="row" alignItems="center" spacing={0.5} sx={{ lineHeight: 1 }}>
      <span>{label}</span>
      <Tooltip arrow title={hint}>
        <Box
          component="span"
          sx={{ pointerEvents: "auto", display: "inline-flex", cursor: "help", lineHeight: 0 }}
        >
          <InfoOutlinedIcon sx={{ fontSize: 16 }} />
        </Box>
      </Tooltip>
    </Stack>
  );
}

function statusLabel(status: string): string {
  switch (status) {
    case "queued": return uiText("В очереди");
    case "running": return uiText("В работе");
    case "paused": return uiText("На паузе");
    case "completed": return uiText("Готов");
    case "failed": return uiText("Ошибка");
    case "canceled": return uiText("Отменён");
    default: return status;
  }
}

function statusColor(status: string): "default" | "primary" | "success" | "warning" | "error" | "info" {
  switch (status) {
    case "running": return "warning";    // активное действие — амбра filled (полная насыщенность)
    case "completed": return "success";  // зелёный — готово
    case "paused": return "warning";     // outlined-амбра — синхронно с кнопкой «Пауза» (та же color="warning")
    case "failed": return "error";       // красный — ошибка
    case "canceled": return "default";   // серый — отменён пользователем (терминальное)
    case "queued": return "info";        // голубой — нейтральное ожидание
    default: return "default";
  }
}

function progressBarColor(status: string): "primary" | "success" | "warning" | "error" | "info" | "inherit" {
  // Бар = тот же color, что и чип. Для paused добавляется className="lp-paused"
  // в <LinearProgress> — он применит opacity:0.5 и покажет «придержанный» тон.
  // canceled остаётся inherit (серый — терминальное состояние).
  switch (status) {
    case "running": return "warning";
    case "paused": return "warning";
    case "completed": return "success";
    case "failed": return "error";
    case "queued": return "info";
    case "canceled":
    default:
      return "inherit";
  }
}

function pluralizeGroups(n: number): string {
  const mod100 = n % 100;
  const mod10 = n % 10;
  if (mod100 >= 11 && mod100 <= 14) return uiText("групп");
  if (mod10 === 1) return uiText("группа");
  if (mod10 >= 2 && mod10 <= 4) return uiText("группы");
  return uiText("групп");
}

type ProgressSnapshot = {
  isGrouped: boolean;
  processed: number;
  total: number;
  percent: number;
  // Единица измерения для формата «X / Y unit» — всегда родительный множественного
  // (иначе при total=1 в «1 / 100» склеивается как «1 группа» и читается странно).
  unit: "групп" | "строк";
};

function computeReportProgress(r: ReportItem | null | undefined): ProgressSnapshot {
  const isGrouped = Boolean(r?.group_by_column) && (r?.group_total ?? 0) > 0;
  const processed = isGrouped
    ? (r?.group_processed ?? 0)
    : (r?.processed_rows ?? 0);
  const total = isGrouped
    ? (r?.group_total ?? 0)
    : (r?.total_rows ?? 0);
  const percent = total > 0 ? (processed / total) * 100 : 0;
  return {
    isGrouped,
    processed,
    total,
    percent,
    unit: isGrouped ? "групп" : "строк",
  };
}

function formatEta(seconds: number | null | undefined): string | null {
  if (seconds == null || !Number.isFinite(seconds) || seconds <= 0) return null;
  const s = Math.round(seconds);
  if (s < 60) return uiText("Осталось ~{0} сек", s);
  if (s < 3600) return uiText("Осталось ~{0} мин", Math.round(s / 60));
  const hours = Math.floor(s / 3600);
  const minutes = Math.round((s % 3600) / 60);
  return minutes > 0 ? uiText("Осталось ~{0} ч {1} мин", hours, minutes) : uiText("Осталось ~{0} ч", hours);
}

// Компактный формат счётчика токенов для меню юзера: 75 523 505 → "75.5 млн".
// До 1000 — как есть; до миллиона — "X.Y тыс"; от миллиона — "X.Y млн".
// Полное число со «100 523 505 →  100 523 505» доступно через formatTokenExact.
function formatTokenCount(n: number | null | undefined): string {
  const value = Number(n ?? 0);
  if (!Number.isFinite(value) || value <= 0) return "0";
  if (value < 1000) return String(Math.round(value));
  if (value < 1_000_000) {
    const thousands = value / 1000;
    return thousands < 10 ? uiText("{0} тыс", thousands.toFixed(1)) : uiText("{0} тыс", Math.round(thousands));
  }
  const millions = value / 1_000_000;
  return millions < 10 ? uiText("{0} млн", millions.toFixed(1)) : uiText("{0} млн", Math.round(millions));
}

// Полное число с разделителями тысяч — для tooltip'а.
function formatTokenExact(n: number | null | undefined): string {
  const value = Number(n ?? 0);
  if (!Number.isFinite(value)) return "0";
  return Math.round(value).toLocaleString(uiLocale());
}

const _CURRENT_STEP_DUPLICATE_RE = /^(Обработка|На паузе)\s+\d+\/\d+\s*$/;
// Точные значения current_step, которые по смыслу совпадают со статусом
// (Chip уже это показывает) — чтобы не дублировать визуально.
const _CURRENT_STEP_DUPLICATE_EXACT = new Set<string>([
  "В очереди",
  "Завершено",
  "Готово",
  "Отменено пользователем",
  "Отменено",
  "Ошибка",
  "На паузе",
]);
function visibleCurrentStep(step: string | null | undefined): string | null {
  if (!step) return null;
  const trimmed = step.trim();
  if (!trimmed) return null;
  // Дублирует прогресс-бар — "Обработка 675/1243", "На паузе 675/1243".
  if (_CURRENT_STEP_DUPLICATE_RE.test(trimmed)) return null;
  // Точное совпадение со статусом ("В очереди", "Завершено", "Отменено пользователем").
  if (_CURRENT_STEP_DUPLICATE_EXACT.has(trimmed)) return null;
  return uiText(trimmed);
}

function createFieldId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `field_${Math.random().toString(36).slice(2, 10)}`;
}

function defaultSchemaFields(): SchemaField[] {
  return [];
}

function createCustomField(): SchemaField {
  return {
    id: createFieldId(),
    name: "",
    type: "text",
    textMinLength: "",
    textMaxLength: "",
    numberMin: "",
    numberMax: "",
    numberIntegerOnly: false,
    datetimeMode: "datetime",
    listValues: [],
    listSingle: true,
    listMinItems: "",
    listMaxItems: "",
  };
}

function fieldToExpectedSchema(field: SchemaField): Record<string, unknown> {
  if (field.type === "text") {
    const schema: Record<string, unknown> = { type: "string" };
    if (field.textMinLength.trim()) schema.min_length = Number(field.textMinLength);
    if (field.textMaxLength.trim()) schema.max_length = Number(field.textMaxLength);
    return schema;
  }

  if (field.type === "number") {
    const schema: Record<string, unknown> = { type: field.numberIntegerOnly ? "integer" : "number" };
    const min = parseDecimal(field.numberMin);
    const max = parseDecimal(field.numberMax);
    if (min !== null) schema.min = min;
    if (max !== null) schema.max = max;
    return schema;
  }

  if (field.type === "datetime") {
    return { type: field.datetimeMode === "date" ? "date" : "datetime" };
  }

  const values = field.listValues.map((v) => v.trim()).filter(Boolean);
  if (field.listSingle) {
    return { type: "enum", values };
  }
  const items: Record<string, unknown> = values.length
    ? { type: "string", enum: values }
    : { type: "string" };
  const schema: Record<string, unknown> = { type: "array", items };
  if (field.listMinItems.trim()) schema.min_items = Number(field.listMinItems);
  if (field.listMaxItems.trim()) schema.max_items = Number(field.listMaxItems);
  return schema;
}

function fieldsToExpectedJson(fields: SchemaField[]): Record<string, unknown> {
  const out: Record<string, unknown> = {};
  for (const field of fields) {
    const name = field.name.trim();
    if (!name) continue;
    out[name] = fieldToExpectedSchema(field);
  }
  return out;
}

function schemaToField(name: string, schema: Record<string, unknown>): SchemaField {
  const base = createCustomField();
  base.id = createFieldId();
  base.name = name;

  const schemaType = String(schema.type || "").trim().toLowerCase();
  if (schemaType === "enum") {
    base.type = "list";
    base.listSingle = true;
    base.listValues = Array.isArray(schema.values)
      ? schema.values.map((item) => String(item).trim()).filter(Boolean)
      : [];
    return base;
  }
  if (schemaType === "string") {
    base.type = "text";
    if (typeof schema.min_length === "number") base.textMinLength = String(schema.min_length);
    if (typeof schema.max_length === "number") base.textMaxLength = String(schema.max_length);
    return base;
  }
  if (schemaType === "number" || schemaType === "integer") {
    base.type = "number";
    base.numberIntegerOnly = schemaType === "integer";
    if (typeof schema.min === "number") base.numberMin = String(schema.min);
    if (typeof schema.max === "number") base.numberMax = String(schema.max);
    if (typeof schema.minimum === "number") base.numberMin = String(schema.minimum);
    if (typeof schema.maximum === "number") base.numberMax = String(schema.maximum);
    return base;
  }
  if (schemaType === "date" || schemaType === "datetime") {
    base.type = "datetime";
    base.datetimeMode = schemaType === "date" ? "date" : "datetime";
    return base;
  }
  if (schemaType === "array") {
    base.type = "list";
    base.listSingle = false;
    const items = schema.items && typeof schema.items === "object" && !Array.isArray(schema.items)
      ? (schema.items as Record<string, unknown>)
      : {};
    if (Array.isArray(items.enum)) {
      base.listValues = (items.enum as unknown[]).map((v) => String(v).trim()).filter(Boolean);
    }
    if (typeof schema.min_items === "number") base.listMinItems = String(schema.min_items);
    if (typeof schema.max_items === "number") base.listMaxItems = String(schema.max_items);
    return base;
  }
  // boolean / object / прочее — маппим в text, чтобы старые пресеты не падали.
  base.type = "text";
  return base;
}

function expectedJsonToFields(expectedJson: Record<string, unknown>): SchemaField[] {
  const ordered: SchemaField[] = [];
  for (const [name, rawSchema] of Object.entries(expectedJson)) {
    if (!rawSchema || Array.isArray(rawSchema) || typeof rawSchema !== "object") {
      throw new Error(uiText("Поле '{0}' не является объектом схемы.", name));
    }
    ordered.push(schemaToField(name, rawSchema as Record<string, unknown>));
  }
  return ordered;
}

function validateExpectedJsonTemplate(payload: Record<string, unknown>): string | null {
  const keys = Object.keys(payload);
  if (!keys.length) return uiText("Ожидаемый JSON не должен быть пустым — добавьте хотя бы одно поле.");
  const validateFieldSchema = (fieldName: string, value: unknown): string | null => {
    if (!value || Array.isArray(value) || typeof value !== "object") {
      return uiText("Поле '{0}' должно быть объектом схемы, например {\"type\":\"string\"}.", fieldName);
    }
    const schema = value as Record<string, unknown>;
    const schemaType = typeof schema.type === "string" ? schema.type.trim().toLowerCase() : "";
    if (!schemaType) return uiText("Поле '{0}' должно содержать ключ 'type'.", fieldName);

    if (schemaType === "string") {
      if ("min_length" in schema && (!Number.isInteger(schema.min_length) || Number(schema.min_length) < 0)) {
        return uiText("Поле '{0}': min_length должен быть целым числом >= 0.", fieldName);
      }
      if ("max_length" in schema && (!Number.isInteger(schema.max_length) || Number(schema.max_length) < 1)) {
        return uiText("Поле '{0}': max_length должен быть целым числом >= 1.", fieldName);
      }
      return null;
    }

    if (schemaType === "date" || schemaType === "datetime") {
      return null;
    }

    if (schemaType === "number" || schemaType === "integer") {
      if ("min" in schema && typeof schema.min !== "number") {
        return uiText("Поле '{0}': min должен быть числом.", fieldName);
      }
      if ("max" in schema && typeof schema.max !== "number") {
        return uiText("Поле '{0}': max должен быть числом.", fieldName);
      }
      return null;
    }

    if (schemaType === "boolean") {
      return null;
    }

    if (schemaType === "enum") {
      if (!Array.isArray(schema.values) || !schema.values.length) {
        return uiText("Поле '{0}' с type=enum должно содержать непустой массив values.", fieldName);
      }
      if (schema.values.some((item) => typeof item !== "string" || !item.trim())) {
        return uiText("Поле '{0}' с type=enum должно содержать только непустые строки в values.", fieldName);
      }
      return null;
    }

    if (schemaType === "array") {
      if (!schema.items || Array.isArray(schema.items) || typeof schema.items !== "object") {
        return uiText("Поле '{0}' с type=array должно содержать объект items.", fieldName);
      }
      return validateFieldSchema(`${fieldName}[]`, schema.items);
    }

    if (schemaType === "object") {
      if (!("properties" in schema)) return null;
      if (!schema.properties || Array.isArray(schema.properties) || typeof schema.properties !== "object") {
        return uiText("Поле '{0}' с type=object должно содержать объект properties.", fieldName);
      }
      const properties = schema.properties as Record<string, unknown>;
      for (const [childKey, childValue] of Object.entries(properties)) {
        if (!childKey.trim()) return uiText("Поле '{0}' содержит пустой ключ в properties.", fieldName);
        const nestedError = validateFieldSchema(`${fieldName}.${childKey}`, childValue);
        if (nestedError) return nestedError;
      }
      if ("required" in schema) {
        if (!Array.isArray(schema.required) || schema.required.some((item) => typeof item !== "string")) {
          return uiText("Поле '{0}': required должен быть массивом строк.", fieldName);
        }
      }
      return null;
    }

    return uiText("Поле '{0}': неподдерживаемый type='{1}'.", fieldName, schemaType);
  };

  for (const key of keys) {
    if (!key.trim()) return uiText("В Ожидаемом JSON есть пустой ключ.");
    const error = validateFieldSchema(key, payload[key]);
    if (error) return error;
  }
  return null;
}

function fallbackReadyPreset(
  hint: ReadyPresetId,
): { prompt: string; expected: Record<string, unknown> } {
  if (getUiLanguage() === "en") return englishReadyPreset(hint === "fraud_by_store");
  if (hint === "fraud_individual") {
    return {
      prompt:
        "Ты аналитик службы внутренней безопасности розничной сети.\n" +
        "По одному отзыву клиента определи, есть ли в нём признаки мошеннических действий сотрудников (обман/присвоение денег), и кратко опиши суть.\n\n" +
        "Шаг 1. Заполни поле «категория» одним из значений:\n" +
        "- «мошенничество» — в отзыве описано или явно подразумевается нарушение сотрудника: оплата мимо кассы (перевод на личную карту/телефон), вымогательство денег, продажа без чека за наличные, «личные скидки» за откат, подмена товара, подделка чеков возврата.\n" +
        "- «плохой сервис» — хамство, медленное обслуживание, грязь, не отвечали на звонки, оскорбления, долгое ожидание — но без признаков мошенничества.\n" +
        "- «брак товара» — проблема с качеством товара/еды: брак, просрочка, несоответствие описанию.\n" +
        "- «позитивный» — клиент доволен.\n" +
        "- «нейтральный» — смешанный или нейтральный отзыв без явных проблем.\n\n" +
        "Шаг 2. Заполни поле «тип_нарушения». Если «категория» = «мошенничество» — выбери конкретный тип:\n" +
        "- «оплата мимо кассы» — перевод на личный счёт, карту или телефон сотрудника в обход кассы.\n" +
        "- «вымогательство» — требование денег за услугу, ускорение, пропуск.\n" +
        "- «личные скидки» — «скидка через мою карту» в обход кассы (обычно за откат).\n" +
        "- «подделка возвратов» — подделка чеков возврата, присвоение части денег.\n" +
        "- «подмена товара» — продажа брака как нового, подмена модели/комплектации.\n" +
        "- «иное мошенничество» — другое нарушение сотрудника, не подходящее под перечисленные.\n" +
        "- «нет мошенничества» — используй это значение, если «категория» ≠ «мошенничество».\n\n" +
        "Шаг 3. Заполни поле «уверенность» числом от 0 до 1. Низкая уверенность (< 0.6) — если формулировка мутная и точно сказать нельзя.\n\n" +
        "Шаг 4. Заполни поле «описание» — 1-2 предложения по-русски, до 240 символов. Кратко суть отзыва и почему ты так классифицировал.\n\n" +
        "Важно: будь строг. Если нет явного признака мошенничества — не записывай в «мошенничество». Просто плохой сервис или некачественный товар — это не мошенничество.\n",
      expected: {
        "категория": {
          type: "enum",
          values: ["мошенничество", "плохой сервис", "брак товара", "позитивный", "нейтральный"],
        },
        "тип_нарушения": {
          type: "enum",
          values: [
            "нет мошенничества",
            "оплата мимо кассы",
            "вымогательство",
            "личные скидки",
            "подделка возвратов",
            "подмена товара",
            "иное мошенничество",
          ],
        },
        "уверенность": { type: "number", min: 0, max: 1 },
        "описание": { type: "string", max_length: 240 },
      },
    };
  }
  if (hint === "fraud_by_store") {
    return {
      prompt:
        "Ты аналитик службы внутренней безопасности розничной сети.\n" +
        "На вход приходит ГРУППА отзывов клиентов одного магазина (обычно одна смена). Твоя задача — оценить ситуацию по магазину: есть ли признаки мошенничества сотрудников, насколько массово, какие конкретно схемы встречаются.\n\n" +
        "Шаг 1. Посчитай «число_нарушений» — сколько отзывов в группе ЯВНО описывают мошенничество сотрудника. Считай только случаи, где клиент прямо описывает одну из схем:\n" +
        "- «оплата мимо кассы» — перевод на личную карту/телефон/СБП сотрудника в обход кассы.\n" +
        "- «вымогательство» — сотрудник требует деньги за услугу, ускорение, пропуск.\n" +
        "- «личные скидки» — «скидка через мою карту/знакомого» в обход кассы.\n" +
        "- «подделка возвратов» — оформили возврат, но часть суммы присвоили.\n" +
        "- «подмена товара» — продали брак/другую модель как новый, с умыслом.\n" +
        "НЕ считай нарушением: хамство, медленное обслуживание, грязь, брак товара без признаков умысла, общее недовольство. Если формулировка мутная — НЕ засчитывай.\n\n" +
        "Шаг 2. «доля_нарушений_процент» — процент от общего числа отзывов в группе, округли до 1 знака.\n\n" +
        "Шаг 3. «схемы_нарушений» — массив уникальных тегов ТОЛЬКО из тех схем, которые реально встретились в отзывах этой группы. Разрешённые значения: «оплата мимо кассы», «вымогательство», «личные скидки», «подмена товара», «подделка возвратов». Если ничего не нашёл — пустой массив []. НЕ копируй весь список из инструкции.\n\n" +
        "Шаг 4. «уровень_риска»:\n" +
        "- «низкий» — 0–1 подтверждённый случай ИЛИ доля < 10%.\n" +
        "- «средний» — 2–4 подтверждённых случая И доля 10–20%.\n" +
        "- «высокий» — 5+ подтверждённых случаев ИЛИ доля > 20% ИЛИ зафиксировано вымогательство (даже 1 случай).\n" +
        "При конфликте условий выбирай более высокий уровень, только если есть именно вымогательство; иначе — более низкий.\n\n" +
        "Шаг 5. «описание» — 2–3 предложения по-русски, до 400 символов. Требования:\n" +
        "- перечисляй ТОЛЬКО схемы, которые реально есть в группе (как в «схемы_нарушений»);\n" +
        "- не используй шаблонные фразы «В магазине выявлены признаки систематического мошенничества»;\n" +
        "- если нарушений мало/нет — так и напиши («единичный случай», «признаков мошенничества не обнаружено»);\n" +
        "- укажи количество подтверждённых случаев и долю; рекомендации давай только при «среднем»/«высоком» риске.\n",
      expected: {
        "число_нарушений": { type: "integer", min: 0 },
        "доля_нарушений_процент": { type: "number", min: 0, max: 100 },
        "схемы_нарушений": {
          type: "array",
          items: { type: "string" },
        },
        "уровень_риска": { type: "enum", values: ["низкий", "средний", "высокий"] },
        "описание": { type: "string", max_length: 400 },
      },
    };
  }
  // Неизвестный id — возвращаем fraud_individual как дефолт.
  return fallbackReadyPreset("fraud_individual");
}

type AllowedValuesInputProps = {
  values: string[];
  onChange: (next: string[]) => void;
};

export function splitAllowedValuesPaste(raw: string): string[] {
  if (!raw) return [];
  return raw
    .split(/[\t\r\n,;]+/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function AllowedValuesInput({ values, onChange }: AllowedValuesInputProps) {
  const handlePaste = (event: ClipboardEvent<HTMLDivElement>) => {
    const pasted = event.clipboardData.getData("text");
    const parts = splitAllowedValuesPaste(pasted);
    if (parts.length <= 1) return;
    event.preventDefault();
    const seen = new Set(values);
    const merged = [...values];
    for (const item of parts) {
      if (seen.has(item)) continue;
      seen.add(item);
      merged.push(item);
    }
    onChange(merged);
  };

  return (
    <Autocomplete
      multiple
      freeSolo
      options={[] as string[]}
      value={values}
      onChange={(_, next) => {
        const cleaned = (next as string[])
          .map((v) => (v ?? "").trim())
          .filter(Boolean);
        const unique: string[] = [];
        const seen = new Set<string>();
        for (const v of cleaned) {
          if (seen.has(v)) continue;
          seen.add(v);
          unique.push(v);
        }
        onChange(unique);
      }}
      renderTags={(tagValues, getTagProps) =>
        tagValues.map((value, index) => {
          const { key, ...tagProps } = getTagProps({ index });
          return <Chip key={key} label={value} size="small" {...tagProps} />;
        })
      }
      renderInput={(params) => (
        <TextField
          {...params}
          label={uiText("Допустимые значения")}
          placeholder={uiText("Введите значение и нажмите Enter. Можно вставить колонку из Excel.")}
          helperText={uiText("Enter — добавить. Ctrl+V из Excel или списка через запятую/точку с запятой — каждое значение станет отдельным чипом.")}
          onPaste={handlePaste}
        />
      )}
    />
  );
}

/* ──────────── Stage 2 helpers: count-up, SVG ring, log line parser ──────────── */

/**
 * useAnimatedNumber — плавный tween целевого числа с ease-out cubic.
 * Возвращает текущее анимируемое значение. При смене target плавно
 * переходит от текущего показанного к новому за `duration` мс.
 */
function useAnimatedNumber(target: number, duration = 600): number {
  const [display, setDisplay] = useState(target);
  const fromRef = useRef(target);
  const startRef = useRef<number | null>(null);
  const rafRef = useRef<number | null>(null);

  useEffect(() => {
    fromRef.current = display;
    startRef.current = null;

    const tick = (now: number) => {
      if (startRef.current === null) startRef.current = now;
      const elapsed = now - startRef.current;
      const t = Math.min(1, elapsed / duration);
      const eased = 1 - Math.pow(1 - t, 3); // ease-out cubic
      const next = fromRef.current + (target - fromRef.current) * eased;
      setDisplay(next);
      if (t < 1) {
        rafRef.current = requestAnimationFrame(tick);
      }
    };

    rafRef.current = requestAnimationFrame(tick);
    return () => {
      if (rafRef.current) cancelAnimationFrame(rafRef.current);
    };
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [target, duration]);

  return display;
}

function AnimatedNumber({
  value,
  format,
  className,
  duration = 600,
}: {
  value: number;
  format?: (n: number) => string;
  className?: string;
  duration?: number;
}) {
  const animated = useAnimatedNumber(value, duration);
  const text = format
    ? format(animated)
    : Math.round(animated).toLocaleString(uiLocale());
  return <span className={className}>{text}</span>;
}

type RingStatus = "idle" | "queued" | "running" | "paused" | "completed" | "failed" | "canceled";

/**
 * RingProgress — SVG-кольцо с центральной подписью и градиентной заливкой.
 * Цвет заливки выбирается по статусу (warning/success/error/info/inherit).
 * Гасит motion при prefers-reduced-motion (наследуется от CSS).
 */
function RingProgress({
  percent,
  size = 152,
  strokeWidth = 10,
  status = "idle",
  centerLabel,
  centerSubLabel,
}: {
  percent: number;
  size?: number;
  strokeWidth?: number;
  status?: RingStatus;
  centerLabel?: React.ReactNode;
  centerSubLabel?: React.ReactNode;
}) {
  const uid = useId().replace(/:/g, "");
  const radius = (size - strokeWidth) / 2;
  const circumference = 2 * Math.PI * radius;
  const clamped = Math.max(0, Math.min(100, percent));
  const offset = circumference * (1 - clamped / 100);

  const gradId = `ring-grad-${status}-${uid}`;
  const gradStops: Record<RingStatus, [string, string]> = {
    running: ["#fb923c", "#ea580c"],
    paused: ["#fdba74", "#ea580c"],
    completed: ["#34d399", "#10b981"],
    failed: ["#f87171", "#dc2626"],
    queued: ["#60a5fa", "#2563eb"],
    canceled: ["#cbd5e1", "#94a3b8"],
    idle: ["#cbd5e1", "#94a3b8"],
  };
  const [from, to] = gradStops[status];

  return (
    <div className={`ring-progress ring-status-${status}`} style={{ width: size, height: size }}>
      <svg width={size} height={size} viewBox={`0 0 ${size} ${size}`} aria-hidden>
        <defs>
          <linearGradient id={gradId} x1="0%" y1="0%" x2="100%" y2="100%">
            <stop offset="0%" stopColor={from} />
            <stop offset="100%" stopColor={to} />
          </linearGradient>
        </defs>
        <circle
          className="ring-track"
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          strokeWidth={strokeWidth}
        />
        <circle
          className="ring-fill"
          cx={size / 2}
          cy={size / 2}
          r={radius}
          fill="none"
          strokeWidth={strokeWidth}
          stroke={`url(#${gradId})`}
          strokeLinecap="round"
          strokeDasharray={circumference}
          strokeDashoffset={offset}
          transform={`rotate(-90 ${size / 2} ${size / 2})`}
        />
      </svg>
      <div className="ring-center">
        <div className="ring-main">{centerLabel}</div>
        {centerSubLabel ? <div className="ring-sub">{centerSubLabel}</div> : null}
      </div>
    </div>
  );
}

/** Категоризирует строку лога по подстрокам — для цветовой полоски слева. */
function classifyLogLine(line: string): "info" | "warn" | "error" | "debug" | "default" {
  const lower = line.toLowerCase();
  if (/\b(error|err|exception|traceback|critical|fatal|✗)\b/.test(lower)) return "error";
  if (/\b(warn|warning|⚠)\b/.test(lower)) return "warn";
  if (/\b(debug|trace)\b/.test(lower)) return "debug";
  if (/\b(info|started|done|готово|✓|обработано|completed|success)\b/.test(lower)) return "info";
  return "default";
}

function App() {
  const [providers, setProviders] = useState<Provider[]>([]);
  const [modelCatalogReady, setModelCatalogReady] = useState(false);
  const [models, setModels] = useState<ModelChoice[]>([]);

  const [user, setUser] = useState<User | null>(null);
  const [accessState, setAccessState] = useState<AccessState | null>(null);
  const accessStateRef = useRef<AccessState | null>(null);
  const activeUserRef = useRef<number | null>(null);
  const lastUsageStartRef = useRef(-Infinity);
  const loadUsageRef = useRef<() => Promise<void>>(async () => undefined);
  const [accessGroupId, setAccessGroupId] = useState<number | "">("");
  const [requestGroupOpen, setRequestGroupOpen] = useState(false);
  const [selectedReportMeta, setSelectedReportMeta] = useState<ReportItem | null>(null);
  const [quotaFailure, setQuotaFailure] = useState<{ message: string; alternative: AccessGroup | null } | null>(null);
  const [launchError, setLaunchError] = useState("");
  const quotaSectionRef = useRef<HTMLDivElement>(null);
  const launchFeedbackRef = useRef<HTMLDivElement>(null);
  useEffect(() => {
    if (!quotaFailure && !launchError) return;
    const frame = requestAnimationFrame(() => revealLaunchElement(launchFeedbackRef.current));
    return () => cancelAnimationFrame(frame);
  }, [quotaFailure, launchError]);
  function failLaunch(message: string) { setError(""); setLaunchError(message); }
  function showQuota() {
    requestAnimationFrame(() => revealLaunchElement(quotaSectionRef.current, '[role="combobox"]'));
  }
  const quotaGroups = accessState?.groups || [];
  const quotaBlockMessage = quotaBlockedReason(quotaGroups, accessGroupId);
  const quotaBlocked = quotaBlockMessage !== null;
  function applyAccessState(state: AccessState) {
    rememberPendingRequests(state.requests);
    const previous = accessStateRef.current;
    accessStateRef.current = state;
    setAccessState(state);
    setAccessGroupId(prev => retainQuotaSelection(previous?.groups ?? null, state.groups, prev, state.last_group_id));
  }
  const locationHash = useLocationHash();
  const canManage = user?.role === "admin" || !!accessState?.is_manager;
  useEffect(() => {
    if (user && accessState && !canManage && locationHash.startsWith("#/admin")) navigate("", true);
  }, [user, accessState, canManage, locationHash]);
  const [settingsWarnings, setSettingsWarnings] = useState<string[]>([]);
  const [pendingSettingsPayload, setPendingSettingsPayload] = useState<Record<string, unknown> | null>(null);
  const [promptPreview, setPromptPreview] = useState<{ messages: Array<{ role: string; content: string }>; response_schema: unknown; truncated: boolean; will_send: boolean } | null>(null);
  const [previewLoading, setPreviewLoading] = useState(false);
  const [promptFullScreen, setPromptFullScreen] = useState(false);
  const [reportName, setReportName] = useState("");
  const [checkedReportIds, setCheckedReportIds] = useState<string[]>([]);
  const [bulkDeleting, setBulkDeleting] = useState(false);
  const [bulkErrors, setBulkErrors] = useState<Array<{ id: string; message?: string }>>([]);
  const [authConfig, setAuthConfig] = useState<AuthConfig | null>(null);
  const [authMode, setAuthMode] = useState<"login" | "register">("login");
  const [authUsername, setAuthUsername] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authLoading, setAuthLoading] = useState(false);

  const [reports, setReports] = useState<ReportItem[]>([]);
  const reportsRef = useRef<ReportItem[]>([]);
  useEffect(() => { reportsRef.current = reports; }, [reports]);
  const [usage, setUsage] = useState<Usage | null>(null);
  const [presets, setPresets] = useState<UserPreset[]>([]);
  const [presetName, setPresetName] = useState("");
  const [presetPickerValue, setPresetPickerValue] = useState("ready:fraud_individual");

  const [fileInfo, setFileInfo] = useState<FileInspectResponse | null>(null);
  const [fileInspectStatus, setFileInspectStatus] = useState<"idle" | "queued" | "parsing" | "ready" | "error">("idle");
  const [fileInspectMessage, setFileInspectMessage] = useState("");
  const [dragActive, setDragActive] = useState(false);
  const [examplesOpen, setExamplesOpen] = useState(false);
  const [examples, setExamples] = useState<ExampleFile[]>([]);
  const [examplesLoading, setExamplesLoading] = useState(false);
  const [exampleLoadingName, setExampleLoadingName] = useState<string | null>(null);
  const [userMenuAnchor, setUserMenuAnchor] = useState<null | HTMLElement>(null);
  const [rulesOpen, setRulesOpen] = useState(false);
  const [releaseNotesOpen, setReleaseNotesOpen] = useState(false);
  const [releases, setReleases] = useState<ReleaseEntry[]>([]);
  const [releasesLoading, setReleasesLoading] = useState(false);
  const [releasesError, setReleasesError] = useState<string | null>(null);
  const [activeReleaseTab, setActiveReleaseTab] = useState(0);
  const releaseNotesContentRef = useRef<HTMLDivElement | null>(null);
  const [sheetName, setSheetName] = useState("");
  const [analysisColumns, setAnalysisColumns] = useState<string[]>([]);
  const [nonAnalysisColumns, setNonAnalysisColumns] = useState<string[]>([]);
  const [groupByColumn, setGroupByColumn] = useState("");

  const restoredModelSelection = useRef({ provider: localStorage.getItem(LAST_PROVIDER_KEY), model: localStorage.getItem(LAST_MODEL_KEY) });
  const [provider, setProvider] = useState(() => localStorage.getItem(LAST_PROVIDER_KEY) || "");
  const [model, setModel] = useState(() => localStorage.getItem(LAST_MODEL_KEY) || "");
  const analysisMode: "custom" = "custom";
  const [apiKey, setApiKey] = useState(sessionStorage.getItem(TOKEN_SESSION_KEY) || "");
  const [rememberToken, setRememberToken] = useState(true);
  const [tokenVerifyLoading, setTokenVerifyLoading] = useState(false);
  const [tokenVerifyResult, setTokenVerifyResult] = useState<VerifyTokenResult | null>(null);
  const [maxReviewsInput, setMaxReviewsInput] = useState("100");
  const [parallelism, setParallelism] = useState(3);
  const [parallelismMax, setParallelismMax] = useState(20);
  const [temperature, setTemperature] = useState(0);
  const [useCache, setUseCache] = useState(true);
  const [saveApiKeyForResume, setSaveApiKeyForResume] = useState(true);

  const [promptEditorValue, setPromptEditorValue] = useState("");
  const [schemaFields, setSchemaFields] = useState<SchemaField[]>(defaultSchemaFields());
  const [schemaBuilderError, setSchemaBuilderError] = useState<string | null>(null);
  const [schemaBuilderExpanded, setSchemaBuilderExpanded] = useState(true);
  const [templateInfoExpanded, setTemplateInfoExpanded] = useState(false);
  const [showAdvancedSchemaAttrs, setShowAdvancedSchemaAttrs] = useState(false);

  const [selectedReportId, setSelectedReportId] = useState<string | null>(null);
  const [selectedReportAnalysis, setSelectedReportAnalysis] = useState<ReportAnalysis | null>(null);
  const [selectedReportLoading, setSelectedReportLoading] = useState(false);
  const [selectedReportLoadingId, setSelectedReportLoadingId] = useState<string | null>(null);
  const resultsBlockRef = useRef<HTMLDivElement | null>(null);
  const scrolledReportIdRef = useRef<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const [showAllReports, setShowAllReports] = useState(false);
  const [error, setError] = useState<string>("");
  const [actionToast, setActionToast] = useState<{ message: string; severity: "success" | "warning" | "error" } | null>(null);
  const [deletingAllReports, setDeletingAllReports] = useState(false);
  const [downloadDialog, setDownloadDialog] = useState<{
    reportId: string;
    jobId: string | null;
    status: string;
    hasResults: boolean;
    hasRaw: boolean;
    hasSource: boolean;
    format: "xlsx" | "raw" | "source";
    filename: string;
  } | null>(null);
  const [retryingReportId, setRetryingReportId] = useState<string | null>(null);
  const [logoSrc, setLogoSrc] = useState<string>(BRAND_LOGO_CANDIDATES[0]);
  const { mode, toggle } = useThemeMode();
  // SSE удалён в v2.1.0 — единственный источник правды теперь polling
  // /api/reports/active. Active = есть running/queued/paused, idle = только
  // терминальные. Активный интервал короче, чтобы прогресс-бар выглядел
  // живым (бэкенд персистит состояние раз в ~1 сек).
  const JOB_REPORTS_POLL_ACTIVE_MS = 1500;
  const JOB_REPORTS_POLL_IDLE_MS = 3000;
  // v2.1.2 (BUG-26): интервал поднят с 20 → 60 сек, чтобы не дёргать
  // get_user_usage (тяжёлый SUM по партициям report_rows). В паре с
  // Redis-кэшем 300 сек нагрузка на PG падает с ~60 SQL/мин до ~4 SQL/мин
  // на 20 активных юзерах.
  const USAGE_POLL_MS = 60000;
  const REPORTS_MIN_REFRESH_GAP_MS = 1500;
  const ACTIVE_REPORTS_MIN_REFRESH_GAP_MS = 2000;
  const reportsLoadInFlightRef = useRef(false);
  const lastReportsLoadTsRef = useRef(0);
  const activeReportsLoadInFlightRef = useRef(false);
  const lastActiveReportsLoadTsRef = useRef(0);
  const presetUploadInputRef = useRef<HTMLInputElement | null>(null);
  const expectedJsonObject = useMemo(() => fieldsToExpectedJson(schemaFields), [schemaFields]);

  function applyTemplateState(prompt: string, expected: Record<string, unknown>) {
    setPromptEditorValue(prompt);
    try {
      setSchemaFields(expectedJsonToFields(expected));
      setSchemaBuilderError(null);
    } catch (e) {
      setSchemaBuilderError((e as Error).message);
      setSchemaFields(defaultSchemaFields());
    }
  }

  function updateSchemaField(fieldId: string, patch: Partial<SchemaField>) {
    setSchemaFields((prev) => prev.map((field) => (field.id === fieldId ? { ...field, ...patch } : field)));
  }

  function addSchemaField() {
    setSchemaFields((prev) => [...prev, createCustomField()]);
  }

  function removeSchemaField(fieldId: string) {
    setSchemaFields((prev) => prev.filter((field) => field.id !== fieldId));
  }

  function moveSchemaField(fieldId: string, direction: -1 | 1) {
    setSchemaFields((prev) => {
      const index = prev.findIndex((field) => field.id === fieldId);
      if (index < 0) return prev;
      const targetIndex = index + direction;
      if (targetIndex < 0 || targetIndex >= prev.length) return prev;
      const next = [...prev];
      const [field] = next.splice(index, 1);
      next.splice(targetIndex, 0, field);
      return next;
    });
  }

  function resetTemplateBuilder() {
    const trainingPreset = fallbackReadyPreset("fraud_individual");
    setPresetPickerValue("ready:fraud_individual");
    setPresetName("");
    applyTemplateState(trainingPreset.prompt, trainingPreset.expected);
  }

  function validateSchemaBuilder(fields: SchemaField[]): boolean {
    const names = new Set<string>();
    for (const field of fields) {
      const trimmed = field.name.trim();
      if (!trimmed) {
        setSchemaBuilderError(uiText("У каждого поля должно быть заполнено название."));
        return false;
      }
      if (names.has(trimmed)) {
        setSchemaBuilderError(uiText("Названия полей должны быть уникальны. Повтор: '{0}'.", trimmed));
        return false;
      }
      names.add(trimmed);
      if (field.type === "list" && field.listSingle && field.listValues.length === 0) {
        setSchemaBuilderError(
          uiText("Поле '{0}': для списка с одним значением нужно заполнить допустимые значения.", trimmed),
        );
        return false;
      }
    }
    const validationError = validateExpectedJsonTemplate(fieldsToExpectedJson(fields));
    setSchemaBuilderError(validationError);
    return !validationError;
  }

  function handleLogoError() {
    setLogoSrc((prev) => {
      const index = BRAND_LOGO_CANDIDATES.indexOf(prev as (typeof BRAND_LOGO_CANDIDATES)[number]);
      if (index < 0) return BRAND_LOGO_CANDIDATES[0];
      return BRAND_LOGO_CANDIDATES[Math.min(index + 1, BRAND_LOGO_CANDIDATES.length - 1)];
    });
  }

  function handleErrorClose(_: unknown, reason?: string) {
    if (reason === "clickaway") return;
    setError("");
  }

  function handleSuccessClose(_: unknown, reason?: string) {
    if (reason === "clickaway") return;
    setActionToast(null);
  }

  async function verifyToken() {
    if (provider !== "openai") return;
    setTokenVerifyLoading(true);
    setTokenVerifyResult(null);
    try {
      const result = await api.verifyProviderToken(provider, apiKey.trim() ? apiKey : null);
      setTokenVerifyResult(result);
      if (!result.ok) {
        setError(result.message || uiText("Проверка токена: HTTP {0}", result.status_code));
      }
    } catch (exc) {
      setError(exc instanceof Error ? exc.message : String(exc));
    } finally {
      setTokenVerifyLoading(false);
    }
  }

  async function loadBaseData() {
    const [providerList, promptData] = await Promise.all([api.getProviders(), api.getDefaultPrompt()]);
    setProviders(providerList);
    setModelCatalogReady(true);
    if (providerList.length > 0) {
      const saved = restoredModelSelection.current;
      const choices = saved.provider && providerList.some(p => p.id === saved.provider)
        ? await api.getModels(saved.provider) : [];
      if (!choices.some(m => m.model === saved.model)) localStorage.removeItem(LAST_MODEL_KEY);
      setProvider(restoreProvider(providerList, saved.provider, saved.model, choices));
    }
    setParallelismMax(promptData.parallelismMax);
    setParallelism((prev) => Math.max(1, Math.min(promptData.parallelismMax, prev)));
    if (!promptEditorValue.trim()) {
      const trainingPreset = fallbackReadyPreset("fraud_individual");
      applyTemplateState(trainingPreset.prompt, trainingPreset.expected);
    }
  }

  async function loadReports(force = false) {
    const now = Date.now();
    if (!force) {
      if (reportsLoadInFlightRef.current) return;
      if (now - lastReportsLoadTsRef.current < REPORTS_MIN_REFRESH_GAP_MS) return;
    }
    reportsLoadInFlightRef.current = true;
    try {
      const data = await api.getReports();
      setReports(data);
      lastReportsLoadTsRef.current = Date.now();
    } catch {
      // no-op
    } finally {
      reportsLoadInFlightRef.current = false;
    }
  }

  function refreshReportsClick() {
    void loadReports(true);
    void loadUsage();
  }

  async function loadActiveReports(force = false) {
    const now = Date.now();
    if (!force) {
      if (activeReportsLoadInFlightRef.current) return;
      if (now - lastActiveReportsLoadTsRef.current < ACTIVE_REPORTS_MIN_REFRESH_GAP_MS) return;
    }
    activeReportsLoadInFlightRef.current = true;
    try {
      const activeRows = await api.getActiveReports();
      const activeById = new Map(activeRows.map((row) => [row.id, row]));
      const activeIds = new Set(activeRows.map((row) => row.id));
      const shouldReloadFull = reportsRef.current.some(row =>
        ["running", "queued", "paused"].includes(row.status) && !activeIds.has(row.id));

      setReports((prev) => {
        const next = prev.map((row) => {
          const fresh = activeById.get(row.id);
          if (!fresh) return row;
          return {
            ...row,
            ...fresh,
            // Тяжёлые JSON-поля (схемы, колонки) light-ручка зануляет — берём
            // их из row (они не меняются после старта отчёта).
            // summary_json: light-ручка ТЕПЕРЬ возвращает живой summary
            // (worker обновляет в БД каждые JOB_EVENTS_FALLBACK_POLL_SEC),
            // поэтому используем fresh.summary_json если он не null,
            // иначе fallback на row.summary_json (например, для свежезагруженного отчёта).
            summary_json: fresh.summary_json ?? row.summary_json,
            output_schema_json: row.output_schema_json,
            expected_json_template_json: row.expected_json_template_json,
            input_columns_json: row.input_columns_json,
          };
        });

        const knownIds = new Set(next.map((row) => row.id));
        const appended = activeRows.filter((row) => !knownIds.has(row.id));
        return appended.length ? [...appended, ...next] : next;
      });

      if (shouldReloadFull) {
        await loadReports(true);
        void loadUsage();
      }
    } catch {
      // no-op
    } finally {
      lastActiveReportsLoadTsRef.current = Date.now();
      activeReportsLoadInFlightRef.current = false;
    }
  }

  async function loadPresets() {
    try {
      const items = await api.listPresets();
      setPresets(items);
      if (presetPickerValue.startsWith("user:")) {
        const presetId = presetPickerValue.slice(5);
        if (!items.some((item) => item.id === presetId)) {
          setPresetPickerValue("ready:fraud_individual");
        }
      }
    } catch {
      // no-op
    }
  }

  async function loadUsage() {
    const actorId = activeUserRef.current;
    if (actorId === null) return;
    lastUsageStartRef.current = performance.now();
    try {
      const [data, state] = await Promise.all([api.usage(), accessCall<AccessState>("/access/status")]);
      if (actorId !== activeUserRef.current) return;
      const gainedAccess = accessStateRef.current?.has_access === false && state.has_access;
      setUsage(data);
      setUser(previous => previous && previous.role !== state.role ? { ...previous, role: state.role } : previous);
      applyAccessState(state);
      if (gainedAccess) await Promise.all([loadBaseData(), loadReports(true), loadPresets()]);
    } catch {
      // Следующий опрос повторит обновление статуса.
    }
  }
  loadUsageRef.current = loadUsage;

  async function enterWorkspace(me: User) {
    const state = await accessCall<AccessState>("/access/status");
    activeUserRef.current = me.id;
    rememberPendingRequests(state.requests);
    accessStateRef.current = state;
    setAccessState(state);
    setUser({ ...me, role: state.role });
    setAccessGroupId(chooseQuota(state.groups, state.last_group_id));
    if (!state.has_access) return;
    await loadBaseData();
    await loadReports();
    await loadUsage();
    await loadPresets();
  }

  async function doAuth() {
    setError("");
    setAuthLoading(true);
    try {
      if (authMode === "register") await api.register(authUsername, authPassword);
      else await api.login(authUsername, authPassword);
      setAuthPassword("");
      await enterWorkspace(await api.me());
    } catch (error) {
      setError((error as Error).message);
    } finally {
      setAuthLoading(false);
    }
  }

  useEffect(() => {
    async function bootstrap() {
      const params = new URLSearchParams(window.location.search);
      if (params.has("sso_error")) {
        setError(uiText("Вход через SSO не удался. Попробуйте ещё раз."));
        params.delete("sso_error");
        const query = params.toString();
        window.history.replaceState({}, "", window.location.pathname + (query ? `?${query}` : "") + window.location.hash);
      }
      try {
        setAuthConfig(await api.getAuthConfig());
      } catch (error) {
        setError((error as Error).message);
      }
      try {
        await enterWorkspace(await api.me());
      } catch (error) {
        if (!(error instanceof APIError && error.status === 401)) setError((error as Error).message);
      }
    }

    void bootstrap();
  }, []);

  const hasActiveReports = useMemo(
    () => reports.some((r) => r.status === "running" || r.status === "queued" || r.status === "paused"),
    [reports],
  );
  const hasRunningReports = useMemo(() => reports.some((r) => r.status === "running"), [reports]);
  const activeReportsPollMs = hasRunningReports ? JOB_REPORTS_POLL_ACTIVE_MS : JOB_REPORTS_POLL_IDLE_MS;

  useEffect(() => {
    if (!user || !hasActiveReports) return;
    const timer = window.setInterval(() => {
      void loadActiveReports();
    }, activeReportsPollMs);
    return () => window.clearInterval(timer);
  }, [user, hasActiveReports, activeReportsPollMs]);

  useEffect(() => {
    if (!user) return;
    const timer = window.setInterval(() => {
      void loadUsage();
    }, USAGE_POLL_MS);
    return () => window.clearInterval(timer);
  }, [user]);

  useEffect(() => {
    if (!user) return;
    const refresh = createFocusAccessRefresh(() => loadUsageRef.current(), { lastLoad: () => lastUsageStartRef.current });
    const onReturn = () => { refresh(document.visibilityState === "visible"); };
    window.addEventListener("focus", onReturn);
    document.addEventListener("visibilitychange", onReturn);
    return () => {
      window.removeEventListener("focus", onReturn);
      document.removeEventListener("visibilitychange", onReturn);
    };
  }, [user?.id]);

  useEffect(() => {
    let active = true;
    async function loadModels() {
      if (!user || !accessState?.has_access) return;
      try {
        const available = await api.getProviders();
        if (!active) return;
        setProviders(available); setModelCatalogReady(true);
        if (!available.length) { setModels([]); setModel(""); setProvider(""); return; }
        const nextProvider = available.some(p => p.id === provider) ? provider : available[0].id;
        if (nextProvider !== provider) { setProvider(nextProvider); return; }
        const modelsList = await api.getModels(provider);
        if (!active) return;
        setModels(modelsList);
        const saved = localStorage.getItem(LAST_MODEL_KEY);
        setModel(chooseModel(modelsList, saved));
      } catch (e) {
        if (active) setError((e as Error).message);
      }
    }

    void loadModels();
    const timer = window.setInterval(() => void loadModels(), 20000);
    return () => { active = false; clearInterval(timer); };
  }, [provider, user?.id, accessState?.has_access, locationHash.startsWith("#/admin")]);

  useEffect(() => {
    if (provider === "openai") setParallelism(3);
    if (provider === "ollama") setParallelism(2);
  }, [provider]);

  useEffect(() => {
    if (provider) localStorage.setItem(LAST_PROVIDER_KEY, provider);
  }, [provider]);

  useEffect(() => {
    if (model) localStorage.setItem(LAST_MODEL_KEY, model);
  }, [model]);

  useEffect(() => {
    setParallelism((prev) => Math.max(1, Math.min(parallelismMax, prev)));
  }, [parallelismMax]);

  const selectedSheetColumns = useMemo(() => {
    const sheet = fileInfo?.sheets.find((item) => item.name === sheetName);
    return sheet?.columns || [];
  }, [fileInfo, sheetName]);

  const selectedSheetRows = useMemo(() => {
    const sheet = fileInfo?.sheets.find((item) => item.name === sheetName);
    return sheet?.total_rows || 0;
  }, [fileInfo, sheetName]);

  const selectedGroupCount = useMemo(() => {
    if (!groupByColumn) return null;
    const sheet = fileInfo?.sheets.find((item) => item.name === sheetName);
    const counts = sheet?.unique_counts;
    if (!counts || !(groupByColumn in counts)) return null;
    return counts[groupByColumn];
  }, [fileInfo, sheetName, groupByColumn]);

  useEffect(() => {
    if (selectedSheetRows <= 0) return;
    setMaxReviewsInput((prev) => {
      const parsed = Number(prev);
      if (!Number.isFinite(parsed) || parsed < 1) return String(selectedSheetRows);
      return String(Math.min(parsed, selectedSheetRows));
    });
  }, [selectedSheetRows]);

  async function doLogout() {
    activeUserRef.current = null; accessStateRef.current = null; lastUsageStartRef.current = -Infinity;
    try {
      await api.logout();
    } catch {
      // ignore
    }
    setRulesOpen(false);
    setAuthMode("login");
    setUser(null);
    setAccessState(null); setAccessGroupId(""); setSelectedReportMeta(null);
    setQuotaFailure(null); setLaunchError(""); setRequestGroupOpen(false);
    setReports([]);
    setPresets([]);
    setSelectedReportId(null);
    setSelectedReportAnalysis(null);
  }

  async function openReleaseNotes() {
    setReleaseNotesOpen(true);
    if (releases.length > 0 || releasesLoading) return;
    setReleasesLoading(true);
    setReleasesError(null);
    try {
      const list = await api.getReleaseNotes();
      setReleases(list);
      setActiveReleaseTab(0);
    } catch (err) {
      setReleasesError(releaseNotesErrorMessage(err instanceof APIError ? err.status : undefined));
    } finally {
      setReleasesLoading(false);
    }
  }

  // Закрытие диалога «Заметки о релизе» — единая точка чтобы зафиксировать
  // в localStorage что текущую версию пользователь уже видел. Иначе после
  // обновления приложения мы автоматически откроем диалог при следующем входе.
  function closeReleaseNotes() {
    setReleaseNotesOpen(false);
    try {
      localStorage.setItem(RELEASE_NOTES_SEEN_VERSION_KEY, APP_VERSION);
    } catch {
      // localStorage может быть недоступен (приватный режим, переполнение) —
      // в этом случае просто покажем диалог снова при следующем входе.
    }
  }

  // Автооткрытие «Заметок о релизе» один раз на пользователя после апдейта.
  // Триггер — успешная авторизация (появление user). Сравниваем APP_VERSION
  // c сохранённым `seen_version` в localStorage; если они расходятся —
  // приложение обновилось с момента последнего посещения, показываем
  // диалог. closeReleaseNotes зафиксирует факт показа.
  useEffect(() => {
    if (!user || !accessState?.has_access || releaseNotesOpen) return;
    let seen: string | null = null;
    try {
      seen = localStorage.getItem(RELEASE_NOTES_SEEN_VERSION_KEY);
    } catch {
      return;
    }
    if (shouldAutoShowRelease(!!user, !!accessState?.has_access, releaseNotesOpen, seen, APP_VERSION)) void openReleaseNotes();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [user, accessState?.has_access]);

  async function openExamples() {
    setExamplesOpen(true);
    if (examples.length > 0 || examplesLoading) return;
    setExamplesLoading(true);
    try {
      const list = await api.listExamples();
      setExamples(list);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setExamplesLoading(false);
    }
  }

  async function useExample(name: string) {
    setExampleLoadingName(name);
    try {
      const file = await api.downloadExample(name);
      setExamplesOpen(false);
      await handleFileUpload(file);
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err));
    } finally {
      setExampleLoadingName(null);
    }
  }

  function formatBytes(size: number): string {
    if (size < 1024) return `${size} B`;
    if (size < 1024 * 1024) return `${(size / 1024).toFixed(1)} KB`;
    return `${(size / (1024 * 1024)).toFixed(1)} MB`;
  }

  async function handleFileUpload(file?: File) {
    if (!file) return;
    setReportName(file.name.replace(/\.[^/.]+$/, "").slice(0, 200));
    setError("");
    setFileInspectStatus("queued");
    setFileInspectMessage(uiText("Файл загружен. Подготавливаем структуру..."));
    try {
      const applyReadyInfo = (info: FileInspectResponse) => {
        setFileInfo(info);
        setFileInspectStatus("ready");
        setFileInspectMessage(uiText("Файл готов. Выберите колонки и запустите анализ."));
        const nextSheet = info.suggested_sheet || info.sheets[0]?.name || "";
        const suggested = info.suggested_column || info.sheets[0]?.columns[0] || "";
        setSheetName(nextSheet);
        setAnalysisColumns(suggested ? [suggested] : []);
        setNonAnalysisColumns([]);
        setGroupByColumn("");
        const rows = info.sheets.find((s) => s.name === nextSheet)?.total_rows || 100;
        setMaxReviewsInput(String(rows > 0 ? rows : 100));
      };

      const queued = await api.inspectFile(file);
      if (queued.inspect_status === "ready") {
        applyReadyInfo(queued);
        return;
      }

      setFileInfo(queued);
      const maxPollAttempts = 180;
      for (let attempt = 0; attempt < maxPollAttempts; attempt += 1) {
        await new Promise((resolve) => setTimeout(resolve, 1000));
        const current = await api.getFileInspect(queued.file_id);
        const status = String(current.inspect_status || "queued").toLowerCase();
        if (status === "parsing") {
          setFileInspectStatus("parsing");
            setFileInspectMessage(uiText("Читаем файл и определяем колонки..."));
        } else if (status === "queued") {
          setFileInspectStatus("queued");
          setFileInspectMessage(uiText("Файл в очереди на подготовку..."));
        }
        if (current.inspect_status === "error") {
          throw new Error(current.inspect_error_text || uiText("Ошибка подготовки файла."));
        }
        if (current.inspect_status === "ready") {
          applyReadyInfo(current);
          return;
        }
      }
      throw new Error(uiText("Подготовка файла заняла слишком много времени. Попробуйте позже."));
    } catch (e) {
      setError((e as Error).message);
      setFileInfo(null);
      setFileInspectStatus("error");
      setFileInspectMessage("");
      setSheetName("");
      setAnalysisColumns([]);
      setNonAnalysisColumns([]);
      setGroupByColumn("");
    }
  }

  useEffect(() => {
    if (!selectedSheetColumns.length) {
      setAnalysisColumns([]);
      setNonAnalysisColumns([]);
      setGroupByColumn("");
      return;
    }
    setAnalysisColumns((prev) => {
      const filtered = prev.filter((col) => selectedSheetColumns.includes(col));
      if (filtered.length) return filtered;
      return [selectedSheetColumns[0]];
    });
    setNonAnalysisColumns((prev) => prev.filter((col) => selectedSheetColumns.includes(col)));
  }, [selectedSheetColumns]);

  useEffect(() => {
    if (groupByColumn && !selectedSheetColumns.includes(groupByColumn)) {
      setGroupByColumn("");
    }
  }, [groupByColumn, selectedSheetColumns]);

  useEffect(() => {
    if (!groupByColumn) return;
    setAnalysisColumns((prev) => {
      if (prev.includes(groupByColumn)) return prev;
      return [...prev, groupByColumn];
    });
  }, [groupByColumn]);

  function onDrop(event: React.DragEvent<HTMLDivElement>) {
    event.preventDefault();
    setDragActive(false);
    const file = event.dataTransfer.files?.[0];
    handleFileUpload(file);
  }

  async function startAnalysis() {
    setQuotaFailure(null); setLaunchError("");
    if (!fileInfo) {
      failLaunch(uiText("Сначала загрузите файл XLSX или CSV."));
      return;
    }
    if (quotaBlocked) { failLaunch(uiText("Выберите группу с доступной квотой.")); return; }
    setQuotaFailure(null);
    const promptTemplate = promptEditorValue.trim();
    const expectedJsonTemplate = expectedJsonObject;

    if (!promptTemplate) {
      failLaunch(uiText("Промпт не должен быть пустым."));
      return;
    }
    if (!validateSchemaBuilder(schemaFields)) { failLaunch(uiText("Проверьте поля ответа перед запуском.")); return; }

    const effectiveAnalysisColumns = analysisColumns;
    if (!effectiveAnalysisColumns.length) {
      failLaunch(uiText("Выберите хотя бы одну колонку для анализа."));
      return;
    }
    const effectiveNonAnalysisColumns = nonAnalysisColumns;
    const parsedMaxReviews = Math.floor(Number(maxReviewsInput));
    if (!Number.isFinite(parsedMaxReviews) || parsedMaxReviews < 1) {
      failLaunch(uiText("Лимит отзывов должен быть целым числом от 1."));
      return;
    }
    // Do not hard-cap by inspect total_rows here: for very large files
    // inspect may return an approximate row count.
    const effectiveMaxReviews = parsedMaxReviews;
    const validationError = validateExpectedJsonTemplate(expectedJsonTemplate);
    if (validationError) {
      failLaunch(validationError);
      return;
    }
    if (!providers.some((item) => item.id === provider)) {
      failLaunch(uiText("Провайдер недоступен. Обновите список провайдеров."));
      return;
    }
    if (rememberToken) {
      sessionStorage.setItem(TOKEN_SESSION_KEY, apiKey);
    } else {
      sessionStorage.removeItem(TOKEN_SESSION_KEY);
    }

    failLaunch("");
    setSelectedReportId(null);
    setSelectedReportAnalysis(null);

    await launchWithPayload({
        name: reportName.trim() || null,
        access_group_id: accessGroupId || null,
        file_id: fileInfo.file_id,
        sheet_name: sheetName,
        provider,
        model,
        api_key: provider === "openai" ? (apiKey || null) : null,
        prompt_template: promptTemplate,
        max_reviews: effectiveMaxReviews,
        parallelism,
        temperature,
        use_cache: useCache,
        analysis_mode: analysisMode,
        expected_json_template: expectedJsonTemplate,
        analysis_columns: effectiveAnalysisColumns,
        non_analysis_columns: effectiveNonAnalysisColumns,
        group_by_column: groupByColumn || null,
        save_api_key_for_resume: provider === "openai" ? saveApiKeyForResume : false,
        include_raw_json: true,
      });
  }

  async function launchWithPayload(payload: Record<string, unknown>) {
    try {
      const newJobId = await api.startJob(payload);
      // newJobId совпадает с reportId — выбираем созданный отчёт сразу,
      // чтобы блок «3. Обработка» начал отображать прогресс из polling.
      setSelectedReportId(newJobId);
      // Принудительный рефреш списков: отчёт появится в «Мои отчёты» и в
      // selectedLaunchReport мгновенно, без ожидания периодического тика.
      await loadReports(true);
      void loadActiveReports(true);
    } catch (e) {
      if (e instanceof APIError && e.detail?.code === "limit_exhausted") {
        const state = await accessCall<AccessState>("/access/status").catch(() => accessState);
        if (state) applyAccessState(state);
        const failedId = Number(e.detail.group_id || payload.access_group_id);
        setQuotaFailure({ message: e.message, alternative: state?.groups.find(g => g.id !== failedId && quotaAvailable(g)) || null });
        setError(""); setLaunchError(""); return;
      }
      if (e instanceof APIError && e.detail?.code === "settings_warning") {
        setPendingSettingsPayload(payload);
        setSettingsWarnings((e.detail.fields as string[]) || []);
        return;
      }
      failLaunch((e as Error).message);
    }
  }

  async function resolveSettings(choice: "passthrough" | "analysis" | "keep") {
    if (!pendingSettingsPayload) return;
    const next: Record<string, unknown> = { ...pendingSettingsPayload, settings_confirmed: true };
    if (choice === "analysis") {
      const columns = Array.from(new Set([...(next.analysis_columns as string[]), ...settingsWarnings]));
      next.analysis_columns = columns; setAnalysisColumns(columns);
    }
    if (choice === "passthrough") {
      const columns = Array.from(new Set([...(next.non_analysis_columns as string[] || []), ...settingsWarnings]));
      next.non_analysis_columns = columns; setNonAnalysisColumns(columns);
    }
    setPendingSettingsPayload(null); setSettingsWarnings([]);
    await launchWithPayload(next);
  }

  async function showPromptPreview() {
    if (!fileInfo) { setError(uiText("Сначала загрузите XLSX или CSV")); return; }
    setPreviewLoading(true); setError("");
    try {
      const result = await accessCall<NonNullable<typeof promptPreview>>("/jobs/preview-prompt", "POST", {
        prompt_language: getUiLanguage(),
        file_id: fileInfo.file_id, sheet_name: sheetName, analysis_columns: analysisColumns,
        non_analysis_columns: nonAnalysisColumns, group_by_column: groupByColumn || null,
        provider, model, prompt_template: promptEditorValue.trim(), analysis_mode: analysisMode,
        expected_json_template: expectedJsonObject, max_reviews: Math.max(1, Number(maxReviewsInput) || 100),
        parallelism, temperature,
      });
      setPromptPreview(result);
    } catch (e) { setError((e as Error).message); } finally { setPreviewLoading(false); }
  }

  async function cancelAnalysis() {
    const activeJobId = launchJobId;
    if (!activeJobId) return;
    try {
      await api.cancelJob(activeJobId);
      setActionToast({ message: uiText("Задача отменена."), severity: "error" });
      await loadReports(true);
      void loadActiveReports(true);
    } catch (e) {
      setError((e as Error).message);
    }
  }

  async function deleteCheckedReports() {
    if (!checkedReportIds.length || !window.confirm(uiText("Удалить выбранные отчёты ({0})? Данные использования сохранятся.", checkedReportIds.length))) return;
    setBulkDeleting(true); setBulkErrors([]);
    try {
      const result = await api.bulkDeleteReports(checkedReportIds);
      const failures = result.results.filter(r => r.status === "error");
      setBulkErrors(failures);
      setCheckedReportIds(failures.map(r => r.id));
      if (selectedReportId && result.results.some(r => r.id === selectedReportId && r.status === "deleted")) {
        setSelectedReportId(null); setSelectedReportAnalysis(null);
      }
      await loadReports(true); await loadUsage();
      setActionToast({ message: uiText("Удалено: {0}. Ошибок: {1}.", result.deleted, result.failed), severity: result.failed ? "warning" : "success" });
    } catch (e) { setError((e as Error).message); } finally { setBulkDeleting(false); }
  }

  async function handleReportAction(jobIdValue: string, action: "pause" | "resume" | "cancel") {
    setError("");
    setQuotaFailure(null); setLaunchError("");
    try {
      if (action === "pause") await api.pauseJob(jobIdValue);
      if (action === "resume") await api.resumeJob(jobIdValue);
      if (action === "cancel") await api.cancelJob(jobIdValue);
      if (action === "pause") setActionToast({ message: uiText("Задача поставлена на паузу."), severity: "warning" });
      if (action === "resume") setActionToast({ message: uiText("Задача запущена."), severity: "success" });
      if (action === "cancel") setActionToast({ message: uiText("Задача отменена."), severity: "error" });

      // Polling /api/reports/active заберёт свежий статус через ~1.5 сек —
      // принудительный рефреш делает это мгновенно для лучшего UX.
      await loadReports(true);
      void loadActiveReports(true);
    } catch (e) {
      setError((e as Error).message);
    }
  }

  async function openReport(reportId: string) {
    setError("");
    setSelectedReportLoading(true);
    setSelectedReportLoadingId(reportId);
    try {
      const [analysis, meta] = await Promise.all([api.getReportAnalysis(reportId), accessCall<ReportItem>(`/reports/${reportId}`)]);
      setSelectedReportMeta(meta);
      setSelectedReportId(reportId);
      setSelectedReportAnalysis(analysis);
      // Принудительный рефреш активных отчётов — selectedLaunchReport
      // мгновенно подтянется из обновлённого reports[] для блоков 3/4.
      void loadActiveReports(true);
      // Каждый клик «Открыть» — это явный пользовательский intent проскроллить
      // к блоку «3/4». useEffect ниже скроллит только один раз на смену
      // selectedReportId и пропускает повторные клики по тому же отчёту
      // (защита от джёрка во время live-polling). Здесь — принудительно
      // на каждый клик; requestAnimationFrame ждёт пока DOM отрисуется
      // с новым analysis.
      requestAnimationFrame(() => {
        resultsBlockRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
      });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSelectedReportLoading(false);
      setSelectedReportLoadingId((prev) => (prev === reportId ? null : prev));
    }
  }

  async function deleteReport(reportId: string) {
    if (!window.confirm(uiText("Удалить отчет без возможности восстановления?"))) return;
    setError("");
    try {
      await api.deleteReport(reportId);
      setActionToast({ message: uiText("Отчет удален."), severity: "error" });
      if (selectedReportId === reportId) {
        setSelectedReportId(null);
        setSelectedReportAnalysis(null);
      }
      await loadReports(true);
    } catch (e) {
      setError((e as Error).message);
    }
  }

  async function retryReport(reportId: string, jobIdValue: string) {
    setError("");
    setRetryingReportId(reportId);
    try {
      await api.retryJob(jobIdValue);
      setActionToast({ message: uiText("Перезапуск поставлен в очередь. Кэш включён."), severity: "success" });
      await loadReports(true);
      void loadActiveReports(true);
      if (selectedReportId === reportId) {
        // Сбрасываем кэш analysis — иначе в виджете «4. Результаты» останется
        // старое превью и застывшая статистика до ручного перезахода на отчёт.
        // После null-а live-polling перезапустится и подтянет свежие данные.
        setSelectedReportAnalysis(null);
      }
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setRetryingReportId(null);
    }
  }

  function openDownloadDialog(report: ReportItem, mode: "completed" | "partial" = "completed") {
    const createdDate = report.created_at ? new Date(report.created_at) : new Date();
    const isoDate = !isNaN(createdDate.getTime())
      ? createdDate.toISOString().slice(0, 10)
      : new Date().toISOString().slice(0, 10);
    const shortId = String(report.id).slice(0, 8);
    const defaultName = `report_${shortId}_${isoDate}`;
    const hasResults = Boolean(report.results_file) && mode === "completed";
    const hasRaw = Boolean(report.raw_file) && mode === "completed";
    const hasSource = Boolean(report.uploaded_file_id);
    // Промежуточные xlsx/raw используют общий кэш среза на сервере.
    const defaultFormat: "xlsx" | "raw" | "source" =
      mode === "partial" ? "xlsx" : hasResults ? "xlsx" : hasRaw ? "raw" : "source";
    setDownloadDialog({
      reportId: report.id,
      jobId: report.job_id,
      status: report.status,
      hasResults: mode === "partial" ? true : hasResults,
      hasRaw: mode === "partial" ? true : hasRaw,
      hasSource,
      format: defaultFormat,
      filename: mode === "partial" ? "" : defaultName,
    });
  }

  function submitDownload() {
    if (!downloadDialog) return;
    const { reportId, status, filename, format } = downloadDialog;
    const safeFilename = filename.trim().replace(/[\\/:*?"<>|\x00-\x1f]/g, "_") || "report";
    const isAdmin = user?.role === "admin";
    const isActive = status === "running" || status === "paused";
    const partial = isActive && (format === "xlsx" || format === "raw");
    const base = isAdmin
      ? `${API_ROOT}/admin/reports/${reportId}`
      : `${API_ROOT}/reports/${reportId}`;
    const segment = partial ? "download/partial" : "download";
    const query = partial && !filename.trim() ? "" : `?filename=${encodeURIComponent(safeFilename)}`;
    const url = `${base}/${segment}/${format}${query}`;
    window.open(url, "_blank");
    setDownloadDialog(null);
  }


  async function savePreset() {
    const name = presetName.trim();
    if (!name) {
      setError(uiText("Введите имя пресета."));
      return;
    }
    const promptTemplate = promptEditorValue.trim();
    if (!promptTemplate) {
      setError(uiText("Промпт не должен быть пустым."));
      return;
    }
    if (!validateSchemaBuilder(schemaFields)) return;
    const expected = expectedJsonObject;
    const validationError = validateExpectedJsonTemplate(expected);
    if (validationError) {
      setError(validationError);
      return;
    }
    try {
      const hint = presetPickerValue.startsWith("ready:") ? presetPickerValue.slice(6) : null;
      const saved = await api.savePreset({
        name,
        prompt_template: promptTemplate,
        expected_json_template: expected,
        template_hint: hint,
      });
      await loadPresets();
      setPresetPickerValue(`user:${saved.id}`);
      setError("");
    } catch (e) {
      setError((e as Error).message);
    }
  }

  function downloadPreset() {
    const promptTemplate = promptEditorValue.trim();
    if (!promptTemplate) {
      setError(uiText("Промпт не должен быть пустым."));
      return;
    }
    if (!validateSchemaBuilder(schemaFields)) return;
    const expected = expectedJsonObject;
    const hint = presetPickerValue.startsWith("ready:") ? presetPickerValue.slice(6) : null;
    const payload = {
      format: "review_analyzer_preset_ui_v1",
      name: presetName.trim() || "preset",
      template_hint: hint,
      prompt_template: promptTemplate,
      expected_json_template: expected,
      exported_at: new Date().toISOString(),
    };
    const jsonText = JSON.stringify(payload, null, 2);
    // Add UTF-8 BOM so Windows editors reliably detect encoding.
    const blob = new Blob(["\uFEFF", jsonText], { type: "application/json;charset=utf-8" });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const safeBaseName = payload.name
      .trim()
      .replace(/[<>:"/\\|?*\u0000-\u001F]/g, "")
      .replace(/\s+/g, "_")
      .replace(/\.+$/g, "")
      .slice(0, 80) || "preset";
    const ts = new Date().toISOString().replace(/[:]/g, "-").slice(0, 19);
    a.download = `${safeBaseName}_${ts}.preset.json`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  async function importPresetFile(file?: File) {
    if (!file) return;
    try {
      const text = await file.text();
      const raw = JSON.parse(text) as Record<string, unknown>;
      const promptTemplate = typeof raw.prompt_template === "string" ? raw.prompt_template.trim() : "";
      const expectedTemplate = raw.expected_json_template;
      const importedName = typeof raw.name === "string" ? raw.name.trim() : "";

      if (!promptTemplate) {
        throw new Error(uiText("В файле пресета отсутствует корректный prompt_template."));
      }
      if (!expectedTemplate || typeof expectedTemplate !== "object" || Array.isArray(expectedTemplate)) {
        throw new Error(uiText("В файле пресета отсутствует корректный expected_json_template."));
      }

      const validationError = validateExpectedJsonTemplate(expectedTemplate as Record<string, unknown>);
      if (validationError) {
        throw new Error(validationError);
      }

      applyTemplateState(promptTemplate, expectedTemplate as Record<string, unknown>);
      if (importedName) {
        setPresetName(importedName);
      }
      setPresetPickerValue("");
      setError("");
      setActionToast({ message: uiText("Пресет загружен."), severity: "success" });
    } catch (e) {
      setError(uiText("Не удалось загрузить пресет: {0}", (e as Error).message));
    }
  }

  async function deleteSelectedPreset() {
    if (!presetPickerValue.startsWith("user:")) return;
    const presetId = presetPickerValue.slice(5);
    try {
      await api.deletePreset(presetId);
      await loadPresets();
      setPresetPickerValue("ready:fraud_individual");
    } catch (e) {
      setError((e as Error).message);
    }
  }

  async function applyPresetSelection(nextValue: string) {
    setPresetPickerValue(nextValue);
    if (nextValue.startsWith("user:")) {
      const presetId = nextValue.slice(5);
      const preset = presets.find((item) => item.id === presetId);
      if (!preset) return;
      setPresetName(preset.name);
      applyTemplateState(preset.prompt_template, preset.expected_json_template);
      return;
    }

    if (nextValue.startsWith("ready:")) {
      const hint = nextValue.slice(6);
      if (!(READY_PRESET_IDS as string[]).includes(hint)) return;
      const typedHint = hint as ReadyPresetId;
      const localPreset = fallbackReadyPreset(typedHint);
      applyTemplateState(localPreset.prompt, localPreset.expected);
    }
  }

  async function pauseAnalysis() {
    const activeJobId = launchJobId;
    if (!activeJobId) return;
    try {
      await handleReportAction(activeJobId, "pause");
    } catch {
      // handleReportAction already sets error
    }
  }

  async function deleteAllReports() {
    if (!reports.length) return;
    if (!window.confirm(uiText("Удалить все отчеты ({0}) без возможности восстановления?", reports.length))) return;
    setError("");
    setDeletingAllReports(true);
    try {
      const ids = reports.map((r) => r.id);
      const activeSet = new Set(ids);
      const outcomes = await Promise.allSettled(ids.map((id) => api.deleteReport(id)));
      const deleted = outcomes.filter((x) => x.status === "fulfilled").length;
      const failed = outcomes.length - deleted;

      if (failed === 0) {
        setActionToast({ message: uiText("Удалены все отчеты: {0}.", deleted), severity: "error" });
      } else {
        setActionToast({ message: uiText("Удалено: {0}, не удалено: {1}.", deleted, failed), severity: "warning" });
      }
      if (selectedReportId && activeSet.has(selectedReportId)) {
        setSelectedReportId(null);
        setSelectedReportAnalysis(null);
      }
      await loadReports();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setDeletingAllReports(false);
    }
  }

  async function resumeAnalysis() {
    const activeJobId = launchJobId;
    if (!activeJobId) return;
    try {
      await handleReportAction(activeJobId, "resume");
    } catch {
      // handleReportAction already sets error
    }
  }

  async function deleteCurrentFromLaunchBlock() {
    const reportId = selectedLaunchReport?.job_id || selectedReportId;
    if (!reportId) return;
    await deleteReport(reportId);
  }

  useEffect(() => {
    if (selectedReportLoading) return;
    if (!selectedReportId) return;
    if (!selectedReportAnalysis?.summary) return;
    // Скроллим только один раз при открытии нового отчёта, а не на каждом
    // обновлении данных во время live-поллинга.
    if (scrolledReportIdRef.current === selectedReportId) return;
    scrolledReportIdRef.current = selectedReportId;
    resultsBlockRef.current?.scrollIntoView({ behavior: "smooth", block: "start" });
  }, [selectedReportId, selectedReportAnalysis, selectedReportLoading]);

  useEffect(() => {
    if (!selectedReportId) {
      scrolledReportIdRef.current = null;
    }
  }, [selectedReportId]);

  const selectedLaunchReport = useMemo(() => {
    if (!selectedReportId) return null;
    return reports.find((r) => r.id === selectedReportId) || (selectedReportMeta?.id === selectedReportId ? selectedReportMeta : null);
  }, [reports, selectedReportId, selectedReportMeta]);

  const activeResult = useMemo(() => {
    if (!selectedReportId) return null;
    const isAdmin = user?.role === "admin";
    const isNonTerminal = (status: string | undefined | null): boolean =>
      status === "queued" || status === "running" || status === "paused";
    // Единственный live-источник в v2.1.0 — polling /api/reports/active.
    // Активный отчёт держит summary_json свежим (worker персистит в БД раз
    // в ~1 сек). Для уже завершённых отчётов, выпавших из active-списка,
    // fallback — selectedReportAnalysis (загружается при openReport).
    const liveStatus = selectedLaunchReport?.status as string | undefined;
    const liveSummary: JobSummary | null = selectedLaunchReport?.summary_json ?? null;
    const analysisSummary = selectedReportAnalysis?.summary ?? null;
    const summary = liveSummary ?? analysisSummary;
    if (!summary) return null;
    const status = liveStatus || (selectedReportAnalysis?.status as string | undefined);
    const inProgress = isNonTerminal(status);
    return {
      title: isAdmin ? uiText("Результаты выбранного отчета") : uiText("4. Результаты"),
      summary,
      previewRows: selectedReportAnalysis?.preview_rows ?? [],
      inProgress,
      status: status ?? null,
      resultsFileLink: selectedReportId
        ? (isAdmin
            ? api.adminReportDownloadUrl(selectedReportId, "xlsx")
            : `${API_ROOT}/reports/${selectedReportId}/download/xlsx`)
        : null,
      rawFileLink: selectedReportId
        ? (isAdmin
            ? api.adminReportDownloadUrl(selectedReportId, "raw")
            : `${API_ROOT}/reports/${selectedReportId}/download/raw`)
        : null,
    };
  }, [selectedReportId, selectedReportAnalysis, selectedLaunchReport, user?.role]);

  const previewColumnKeys = useMemo(() => {
    const rows = activeResult?.previewRows ?? [];
    const seen = new Set<string>();
    const keys: string[] = [];
    for (const row of rows) {
      for (const key of Object.keys(row.columns || {})) {
        if (seen.has(key)) continue;
        seen.add(key);
        keys.push(key);
      }
    }
    return keys;
  }, [activeResult?.previewRows]);

  const chartData = useMemo(() => {
    const summary = activeResult?.summary;
    if (!summary) return [];
    const done = Math.max(0, summary.success_rows || 0);
    const failed = Math.max(0, summary.failed_rows || 0);
    return [
      { name: uiText("Успешно"), value: done, fill: "#1f9d69" },
      { name: uiText("С ошибками"), value: failed, fill: "#d14334" },
    ];
  }, [activeResult]);

  const visibleReports = useMemo(() => {
    if (showAllReports) return reports;
    return reports.slice(0, 3);
  }, [reports, showAllReports]);


  // Live-превью: пока отчёт в работе (running/paused/queued), тянем analysis
  // каждые 5 секунд — но только пока в UI ещё нет 10 строк превью. После
  // заполнения таблицы превью поллинг останавливается: данные уже есть,
  // повторные запросы в БД не нужны. Финальные данные подтянутся при
  // переходе в терминальный статус (этот же useEffect отработает ещё раз,
  // т.к. selectedLaunchReport?.status — в зависимостях).
  useEffect(() => {
    if (!selectedReportId) return;
    const liveStatus = selectedLaunchReport?.status;
    if (liveStatus !== "running" && liveStatus !== "paused" && liveStatus !== "queued") return;
    const currentPreviewCount = selectedReportAnalysis?.preview_rows?.length ?? 0;
    if (currentPreviewCount >= 10) return;
    const isAdmin = user?.role === "admin";

    let cancelled = false;
    const fetchAnalysis = async () => {
      try {
        const analysis = isAdmin
          ? await api.adminReportAnalysis(selectedReportId)
          : await api.getReportAnalysis(selectedReportId);
        if (!cancelled) setSelectedReportAnalysis(analysis);
      } catch {
        // ignore: поллинг устойчив к разовым ошибкам
      }
    };
    void fetchAnalysis();
    const handle = window.setInterval(fetchAnalysis, 5000);
    return () => {
      cancelled = true;
      window.clearInterval(handle);
    };
  }, [selectedReportId, selectedLaunchReport?.status, user?.role, selectedReportAnalysis?.preview_rows?.length]);

  const launchJobId = useMemo(() => {
    if (selectedLaunchReport && ["running", "queued", "paused"].includes(selectedLaunchReport.status)) {
      return selectedLaunchReport.job_id;
    }
    return null;
  }, [selectedLaunchReport]);

  const launchStatus = useMemo(() => {
    if (selectedLaunchReport && ["running", "queued", "paused"].includes(selectedLaunchReport.status)) {
      return selectedLaunchReport.status;
    }
    return null;
  }, [selectedLaunchReport]);

  // Статус выбранного отчёта вне зависимости от терминальности: нужен для
  // кнопок Перезапустить/Скачать в блоке «Запуск», которые должны отображаться
  // и на failed/canceled/completed (launchStatus такие статусы намеренно не отдаёт).
  const selectedLaunchStatus = useMemo<string | null>(() => {
    if (selectedLaunchReport?.status) return selectedLaunchReport.status;
    return null;
  }, [selectedLaunchReport]);

  if (!user) {
    return (
      <Box className="page workspace-login">
      <Box component="header" className="workspace-topbar">
        <Box className="workspace-topbar-inner">
          <Box className="workspace-brand">
            <img src={logoSrc} alt="" width={32} height={32} className="workspace-brand-image" onError={handleLogoError} />
            <Typography component="span" className="workspace-wordmark">Analyzer</Typography>
          </Box>
          <Tooltip title={mode === "dark" ? uiText("Светлая тема") : uiText("Тёмная тема")}>
            <IconButton className="workspace-icon-button" onClick={toggle} aria-label={mode === "dark" ? uiText("Светлая тема") : uiText("Тёмная тема")}>
              {mode === "dark" ? <LightModeIcon /> : <DarkModeIcon />}
            </IconButton>
          </Tooltip>
        </Box>
      </Box>
      <Box component="main" className="workspace-login-content">
          <Card className="card login-card" sx={{ width: "100%", maxWidth: 480 }}>
            <CardContent>
              <Stack spacing={2}>
                <Typography variant="h5" component="h1">
                  {authMode === "register" ? uiText("Регистрация") : uiText("Вход")}
                </Typography>
                {authConfig?.local_auth_enabled && (
                  <Stack component="form" spacing={2} onSubmit={(event) => { event.preventDefault(); void doAuth(); }}>
                    <TextField
                      label={uiText("Логин")}
                      autoComplete="username"
                      required
                      value={authUsername}
                      onChange={(event) => setAuthUsername(event.target.value)}
                    />
                    <TextField
                      label={uiText("Пароль")}
                      type="password"
                      autoComplete={authMode === "register" ? "new-password" : "current-password"}
                      required
                      value={authPassword}
                      onChange={(event) => setAuthPassword(event.target.value)}
                      helperText={authMode === "register" ? uiText("От 8 символов, включая букву и цифру") : undefined}
                    />
                    <Button type="submit" variant="contained" disabled={authLoading}>
                      {authMode === "register" ? uiText("Создать аккаунт") : uiText("Войти")}
                    </Button>
                    {authConfig.local_registration_enabled && (
                      <Button onClick={() => setAuthMode(authMode === "login" ? "register" : "login")}>
                        {authMode === "login" ? uiText("Создать аккаунт") : uiText("Уже есть аккаунт")}
                      </Button>
                    )}
                  </Stack>
                )}
                {authConfig?.sso_enabled && (
                  <Stack spacing={1} alignItems="center">
                    <Button fullWidth variant={authConfig.local_auth_enabled ? "outlined" : "contained"} href={api.ssoLoginUrl()}>{uiText("Войти через SSO")}</Button>
                    <Link href={api.ssoLoginUrl(true)} underline="hover" variant="body2">{uiText("Войти через другой аккаунт SSO")}</Link>
                  </Stack>
                )}
                {authConfig && !authConfig.sso_enabled && !authConfig.local_auth_enabled && (
                  <Alert severity="info">{uiText("Вход отключён. Обратитесь к администратору.")}</Alert>
                )}
              </Stack>
            </CardContent>
          </Card>
          <Snackbar
            open={Boolean(error)}
            autoHideDuration={6000}
            onClose={handleErrorClose}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
          >
            <Alert severity="error" variant="filled" onClose={handleErrorClose} sx={{ width: "100%" }}>
              {error}
            </Alert>
          </Snackbar>
        </Box>
      </Box>
    );
  }

  if (!accessState) return <Box sx={{ p: 4 }}><Typography>{uiText("Проверка доступа…")}</Typography></Box>;

  const themeLabel = mode === "dark" ? uiText("Светлая тема") : uiText("Тёмная тема");
  const accountButton = (
    <Stack direction="row" spacing={1} className="workspace-account-tools">
      <Tooltip title={themeLabel}>
        <IconButton className="workspace-icon-button" onClick={toggle} aria-label={themeLabel}>
          {mode === "dark" ? <LightModeIcon /> : <DarkModeIcon />}
        </IconButton>
      </Tooltip>
      <Tooltip title={uiText("Меню пользователя")}>
            <IconButton
              onClick={(e) => setUserMenuAnchor(e.currentTarget)}
              className="workspace-icon-button"
              aria-label={uiText("Меню пользователя")}
              aria-haspopup="menu"
              aria-expanded={Boolean(userMenuAnchor)}
              size="large"
              sx={{
                border: 1,
                borderColor: "divider",
                borderRadius: 1.5,
                width: 44,
                height: 44,
              }}
            >
              <MenuIcon />
            </IconButton>
      </Tooltip>
    </Stack>
  );
  const accountMenu = (<Menu
            anchorEl={userMenuAnchor}
            open={Boolean(userMenuAnchor)}
            onClose={() => setUserMenuAnchor(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            PaperProps={{ sx: { minWidth: 260 } }}
          >
            <Box sx={{ px: 2, py: 1.25 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 600 }}><UserIdentity user={user} /></Typography>
              <Typography variant="caption" color="text.secondary">{uiText("Роль:") + " "}{user.role === "admin" ? uiText("Администратор") : accessState.is_manager ? uiText("Руководитель") : uiText("Пользователь")}
              </Typography>
            </Box>
            <Divider />
            <Box sx={{ px: 2, py: 1 }}>
              <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 0.75 }}>{uiText("Использовано токенов")}</Typography>
              <Box
                sx={{
                  display: "grid",
                  gridTemplateColumns: "auto 1fr",
                  columnGap: 1.25,
                  rowGap: 0.25,
                  "& .label": { color: "text.secondary", fontSize: "0.85rem" },
                  "& .value": { fontVariantNumeric: "tabular-nums", fontSize: "0.9rem" },
                  "& .total": { fontWeight: 600 },
                }}
              >
                <Tooltip title={formatTokenExact(usage?.prompt_tokens)} placement="left" arrow>
                  <Typography component="span" className="label">{uiText("Запрос:")}</Typography>
                </Tooltip>
                <Tooltip title={formatTokenExact(usage?.prompt_tokens)} placement="right" arrow>
                  <Typography component="span" className="value">{formatTokenCount(usage?.prompt_tokens)}</Typography>
                </Tooltip>
                <Tooltip title={formatTokenExact(usage?.completion_tokens)} placement="left" arrow>
                  <Typography component="span" className="label">{uiText("Ответ:")}</Typography>
                </Tooltip>
                <Tooltip title={formatTokenExact(usage?.completion_tokens)} placement="right" arrow>
                  <Typography component="span" className="value">{formatTokenCount(usage?.completion_tokens)}</Typography>
                </Tooltip>
                <Tooltip title={formatTokenExact(usage?.total_tokens)} placement="left" arrow>
                  <Typography component="span" className="label total">{uiText("Всего:")}</Typography>
                </Tooltip>
                <Tooltip title={formatTokenExact(usage?.total_tokens)} placement="right" arrow>
                  <Typography component="span" className="value total">{formatTokenCount(usage?.total_tokens)}</Typography>
                </Tooltip>
              </Box>
            </Box>
            <Divider />
            {canManage && <MenuItem onClick={() => { setUserMenuAnchor(null); navigate("#/admin"); }}>
              <ListItemIcon><SettingsIcon fontSize="small" /></ListItemIcon>
              <ListItemText>{user.role === "admin" ? uiText("Администрирование") : uiText("Управление группами")}</ListItemText>
            </MenuItem>}
            <MenuItem onClick={() => { setUserMenuAnchor(null); setRulesOpen(true); }}>
              <ListItemIcon><InfoOutlinedIcon fontSize="small" /></ListItemIcon>
              <ListItemText>{uiText("Правила работы")}</ListItemText>
            </MenuItem>
            <MenuItem onClick={() => { setUserMenuAnchor(null); openReleaseNotes(); }}>
              <ListItemIcon><NewReleasesIcon fontSize="small" /></ListItemIcon>
              <ListItemText>{uiText("Заметки о релизе")}</ListItemText>
            </MenuItem>
            <Divider />
            <MenuItem onClick={() => { setUserMenuAnchor(null); doLogout(); }}>
              <ListItemIcon><LogoutIcon fontSize="small" color="error" /></ListItemIcon>
              <ListItemText primaryTypographyProps={{ color: "error" }}>{uiText("Выйти")}</ListItemText>
            </MenuItem>
            <Divider />
            <Box sx={{ px: 2, py: 0.75 }}>
              <Typography variant="caption" color="text.secondary">
                {uiText("Analyzer · версия {0}", APP_VERSION)}
              </Typography>
            </Box>
          </Menu>);
  const releaseDialog = (<Dialog
          open={releaseNotesOpen}
          onClose={closeReleaseNotes}
          maxWidth="md"
          fullWidth
          PaperProps={{ sx: { height: "min(80vh, 720px)" } }}
        >
          <DialogTitle sx={{ pb: 0.5 }}>{uiText("Заметки о релизе")}<IconButton
              onClick={closeReleaseNotes}
              sx={{ position: "absolute", right: 8, top: 8 }}
              aria-label={uiText("Закрыть")}
            >
              <CloseIcon fontSize="small" />
            </IconButton>
          </DialogTitle>
          {releases.length > 0 && (
            <Tabs
              value={activeReleaseTab}
              onChange={(_, next) => {
                setActiveReleaseTab(next);
                // Сбрасываем прокрутку при смене вкладки, чтобы новый релиз
                // показывался с начала, а не «оборванным» с того места где
                // юзер остановился в предыдущей вкладке.
                releaseNotesContentRef.current?.scrollTo({ top: 0 });
              }}
              variant="scrollable"
              scrollButtons="auto"
              sx={{ px: 2, borderBottom: 1, borderColor: "divider" }}
            >
              {releases.map((entry, idx) => (
                <Tab
                  key={entry.version}
                  label={
                    idx === 0 ? (
                      <Stack direction="row" spacing={0.75} alignItems="center">
                        <span>{entry.version}</span>
                        <Chip label={uiText("актуальная")} size="small" color="primary" sx={{ height: 20, fontSize: "0.7rem" }} />
                      </Stack>
                    ) : (
                      entry.version
                    )
                  }
                  sx={{ textTransform: "none", fontWeight: 600 }}
                />
              ))}
            </Tabs>
          )}
          <DialogContent ref={releaseNotesContentRef} dividers>
            {releasesLoading && (
              <Stack alignItems="center" sx={{ py: 4 }}>
                <LinearProgress aria-label={uiText("Загрузка заметок о релизе")} sx={{ width: "100%" }} />
                <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>{uiText("Загружаем заметки…")}</Typography>
              </Stack>
            )}
            {!releasesLoading && releasesError && (
              <Typography variant="body2" color="error">
                {releasesError}
              </Typography>
            )}
            {!releasesLoading && !releasesError && releases.length === 0 && (
              <Typography variant="body2" color="text.secondary">{uiText("Заметки о релизе пока не опубликованы.")}</Typography>
            )}
            {!releasesLoading && !releasesError && releases.length > 0 && releases[activeReleaseTab] && (
              <Box>
                <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
                  {releases[activeReleaseTab].title}
                </Typography>
                <UserMarkdown content={releases[activeReleaseTab].content_md} />
              </Box>
            )}
          </DialogContent>
          <DialogActions>
            <Button onClick={closeReleaseNotes} variant="contained">{uiText("Закрыть")}</Button>
          </DialogActions>
        </Dialog>);
  const rulesDialog = <UserRulesDialog open={rulesOpen} onClose={() => setRulesOpen(false)} />;
  if (!accessState.has_access) return <>
    <AccessPage state={accessState} logout={doLogout} logoSrc={logoSrc} accountActions={<>{accountButton}{accountMenu}</>} />
    {releaseDialog}{rulesDialog}
  </>;

  if (canManage && locationHash.startsWith("#/admin")) return <AdminScreen
    isAdmin={user.role === "admin"} userId={user.id} hash={locationHash} menuButton={accountButton} accountMenu={<>{accountMenu}{releaseDialog}{rulesDialog}</>} logoSrc={logoSrc}
    onChanged={() => { void loadUsage(); void accessCall<AccessState>("/access/status").then(applyAccessState); }} />;

  const sections = [
    { id: "data-import", label: uiText("Данные и модель"), icon: <CloudUploadIcon fontSize="small" /> },
    { id: "prompt-template", label: uiText("Шаблон"), icon: <DescriptionIcon fontSize="small" /> },
    { id: "analysis-run", label: uiText("Запуск"), icon: <PlayArrowIcon fontSize="small" /> },
    ...(activeResult?.summary ? [{ id: "analysis-result", label: uiText("Результат"), icon: <DownloadIcon fontSize="small" /> }] : []),
    { id: "my-reports", label: uiText("Мои отчёты"), icon: <FolderOpenIcon fontSize="small" /> },
  ];
  const selectedSection = sections.find((section) => `#${section.id}` === locationHash)?.id ?? sections[0].id;

  return (
    <Box className="page workspace-page">
      <a className="workspace-skip" href="#workspace-main">{uiText("К анализу")}</a>
      <Box component="header" className="workspace-topbar">
        <Box className="workspace-topbar-inner">
          <Box className="workspace-brand">
            <img src={logoSrc} alt="" width={32} height={32} className="workspace-brand-image" onError={handleLogoError} />
            <Typography component="span" className="workspace-wordmark">Analyzer</Typography>
          </Box>
          {accountButton}
          {accountMenu}
        </Box>
      </Box>
        <Box component="main" className="workspace-main workspace-layout" id="workspace-main" tabIndex={-1}>
          <Box className="workspace-intro">
            <Typography component="h1">{uiText("Анализ отзывов")}</Typography>
          </Box>
        <Box component="nav" className="workspace-nav" aria-label={uiText("Разделы анализа")}>
          {sections.map((section) => (
            <a key={section.id} href={`#${section.id}`} aria-current={selectedSection === section.id ? "location" : undefined}>
              {section.icon}
              <span>{section.label}</span>
            </a>
          ))}
          {canManage && (
            <a className="workspace-nav-management" href="#/admin">
              <SettingsIcon fontSize="small" />
              <span>{user.role === "admin" ? uiText("Управление") : uiText("Мои группы")}</span>
            </a>
          )}
        </Box>
          {user.role === "admin" && modelCatalogReady && providers.length === 0 && <ModelSetupNotice isAdmin />}
        {accessState.has_access && (
        <Card className="card workspace-section" id="data-import">
        <CardContent>
          <Dialog open={requestGroupOpen} onClose={() => setRequestGroupOpen(false)} fullWidth maxWidth="sm">
            <DialogTitle>{uiText("Запросить доступ в другую группу")}</DialogTitle>
            <AccessRequestForm state={accessState} secondary onChanged={applyAccessState} onClose={() => setRequestGroupOpen(false)} />
          </Dialog>
            <Box className="workspace-file-heading">
              <Typography variant="h6" component="h2">{uiText("Данные и модель")}</Typography>
              {!fileInfo && (
                <Button variant="text" onClick={openExamples} startIcon={<FolderOpenIcon />} sx={{ minHeight: 44 }}>{uiText("Примеры файлов")}</Button>
              )}
            </Box>
            <input
              ref={fileInputRef}
              hidden
              type="file"
              accept=".xlsx,.csv"
              onChange={(e) => {
                handleFileUpload(e.target.files?.[0]);
                // Сбрасываем value, чтобы onChange сработал даже если пользователь
                // выбрал тот же файл повторно (например, после ошибки).
                if (fileInputRef.current) fileInputRef.current.value = "";
              }}
            />
            <Box
              className={`dropzone ${dragActive ? "active" : ""}`}
              onDragOver={(e) => {
                e.preventDefault();
                setDragActive(true);
              }}
              onDragLeave={() => setDragActive(false)}
              onDrop={onDrop}
            >
              <Box className="workspace-upload-content">
                <Box className="workspace-upload-icon">
                  {fileInfo ? <DescriptionIcon fontSize="small" /> : <CloudUploadIcon fontSize="small" />}
                </Box>
                <Box className="workspace-upload-copy">
                  <Typography className="workspace-upload-label">
                    {fileInfo?.filename ?? uiText("Перетащите файл сюда")}
                  </Typography>
                  {!fileInfo && (
                    <Typography variant="body2" color="text.secondary">{uiText("XLSX или CSV · UTF-8 или Windows-1251")}</Typography>
                  )}
                </Box>
                <Button
                  variant={fileInfo ? "outlined" : "contained"}
                  onClick={() => fileInputRef.current?.click()}
                  className="workspace-choose-file"
                >
                  {fileInfo ? uiText("Заменить файл") : uiText("Выбрать файл")}
                </Button>
              </Box>
            </Box>

            {(fileInspectStatus === "queued" || fileInspectStatus === "parsing") && (
              <Box sx={{ mt: 2 }}>
                <LinearProgress aria-label={uiText("Подготовка файла")} />
                <Typography variant="caption" color="text.secondary" sx={{ mt: 0.75, display: "block" }}>
                  {fileInspectMessage || uiText("Подготовка файла...")}
                </Typography>
                {typeof fileInfo?.queue_position === "number" && (
                  <Typography variant="caption" color="text.secondary" sx={{ display: "block" }}>
                    {formatQueueHint(fileInfo.queue_position)}
                  </Typography>
                )}
              </Box>
            )}

          <Box ref={quotaSectionRef} tabIndex={-1} className="workspace-quota">
            <GroupUsage groups={accessState.groups} selected={accessGroupId}
              onChange={id => { setAccessGroupId(id); setQuotaFailure(null); setLaunchError(""); }}
              actions={<Box sx={{ minWidth: 0, display: "flex", alignItems: "center", flexWrap: "wrap", gap: 1 }}>
                <Button variant="text" color="inherit" size="small" sx={{ textTransform: "none", textDecoration: "underline" }}
                  onClick={() => setRulesOpen(true)}>{uiText("Правила работы")}</Button>
                <RequestAccessButton pending={accessState.requests.some(r => r.status === "pending")} onClick={() => setRequestGroupOpen(true)} />
              </Box>}
              notice={<AccessRequestNotice state={accessState} onChanged={applyAccessState} />} />
          </Box>

            {fileInfo && <TextField fullWidth label={uiText("Название отчёта")} value={reportName} inputProps={{ maxLength: 200 }}
              onChange={e => setReportName(e.target.value)} sx={{ mt: 2 }} helperText={uiText("Название в списках отчётов")} />}

            {fileInfo && (
              <Stack direction={{ xs: "column", md: "row" }} spacing={2} sx={{ mt: 2 }} alignItems="stretch">
                <FormControl fullWidth>
                  <InputLabel id="sheet-label" shrink>
                    <FieldLabelContent label={uiText("Лист")} hint={uiText("Страница Excel-файла, строки которой пойдут в анализ.")} />
                  </InputLabel>
                  <Select
                    labelId="sheet-label"
                    label={uiText("Лист ⓘ")}
                    notched
                    displayEmpty
                    value={sheetName}
                    onChange={(e) => setSheetName(String(e.target.value))}
                  >
                    {fileInfo.sheets.map((sheet) => (
                      <MenuItem key={sheet.name} value={sheet.name}>{sheet.name}</MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <FormControl fullWidth>
                  <InputLabel id="analysis-cols-label" shrink>
                    <FieldLabelContent
                      label={uiText("Колонки для анализа")}
                      hint={uiText("Эти поля отправятся модели на анализ. В итоговый отчёт они не копируются — если нужны в xlsx, добавьте их в «Колонки из исходника в итоговый отчёт».")}
                    />
                  </InputLabel>
                  <Select
                    labelId="analysis-cols-label"
                    label={uiText("Колонки для анализа ⓘ")}
                    notched
                    displayEmpty
                    multiple
                    value={analysisColumns}
                    onChange={(e) => {
                      const value = e.target.value;
                      const next = typeof value === "string" ? value.split(",") : value;
                      setAnalysisColumns(next);
                    }}
                    renderValue={(selected) => (selected as string[]).join(", ")}
                  >
                    {selectedSheetColumns.map((col) => (
                      <MenuItem key={col} value={col}>{col}</MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <FormControl fullWidth>
                  <InputLabel id="non-analysis-cols-label" shrink>
                    <FieldLabelContent
                      label={uiText("Колонки в итоговый отчёт")}
                      hint={groupByColumn
                        ? uiText("В групповом отчёте сохраняются на листе «Исходные данные» с номером группы, без отправки в модель.")
                        : uiText("Копируются в итоговый xlsx без обработки моделью. Можно выбирать те же колонки, что и для анализа — в итоге появятся один раз.")}
                    />
                  </InputLabel>
                  <Select
                    labelId="non-analysis-cols-label"
                    label={uiText("Колонки в итоговый отчёт ⓘ")}
                    notched
                    displayEmpty
                    multiple
                    value={nonAnalysisColumns}
                    onChange={(e) => {
                      const value = e.target.value;
                      const next = typeof value === "string" ? value.split(",") : value;
                      setNonAnalysisColumns(next);
                    }}
                    renderValue={(selected) => (selected as string[]).join(", ")}
                  >
                    {selectedSheetColumns.map((col) => (
                      <MenuItem key={col} value={col}>{col}</MenuItem>
                    ))}
                  </Select>
                </FormControl>

                <FormControl fullWidth>
                  <InputLabel id="group-by-label" shrink>
                    <FieldLabelContent
                      label={uiText("Группировка (опц.)")}
                      hint={uiText("Строки с одинаковым значением этой колонки обрабатываются как одна группа — один запрос в модель на всю группу, результат применяется ко всем строкам.")}
                    />
                  </InputLabel>
                  <Select
                    labelId="group-by-label"
                    label={uiText("Группировка (опц.) ⓘ")}
                    notched
                    displayEmpty
                    value={groupByColumn}
                    onChange={(e) => setGroupByColumn(String(e.target.value))}
                  >
                    <MenuItem value="">{uiText("Без группировки")}</MenuItem>
                    {selectedSheetColumns.map((col) => (
                      <MenuItem key={col} value={col}>{col}</MenuItem>
                    ))}
                  </Select>
                </FormControl>
              </Stack>
            )}

            {fileInfo && (
              <Accordion sx={{ mt: 3, "&::before": { display: "none" } }} disableGutters elevation={0} variant="outlined">
                <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                  <Typography variant="subtitle1" fontWeight={500}>{uiText("Продвинутые настройки LLM")}</Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ ml: 2, alignSelf: "center" }}>{uiText("Провайдер, модель, токен, лимиты, кэш")}</Typography>
                </AccordionSummary>
                <AccordionDetails>
                  <Stack direction={{ xs: "column", md: "row" }} spacing={2} alignItems="stretch">
                    <FormControl fullWidth>
                      <InputLabel id="provider-label" shrink>
                        <FieldLabelContent
                          label={uiText("Провайдер")}
                          hint={uiText("Выбери, куда будет уходить запрос: OpenAI-совместимая платформа или локальный LLM (Ollama).")}
                        />
                      </InputLabel>
                      <Select
                        labelId="provider-label"
                        label={uiText("Провайдер ⓘ")}
                        notched
                        displayEmpty
                        value={provider}
                        onChange={(e) => setProvider(String(e.target.value))}
                      >
                        {providers.map((item) => (
                          <MenuItem key={item.id} value={item.id}>{item.label}</MenuItem>
                        ))}
                      </Select>
                    </FormControl>

                    <FormControl fullWidth>
                      <InputLabel id="model-label" shrink>
                        <FieldLabelContent
                          label={uiText("Модель")}
                          hint={uiText("Конкретная модель выбранного провайдера. Мощнее — точнее, но дороже по токенам и медленнее.")}
                        />
                      </InputLabel>
                      <Select
                        disabled={!models.length}
                        labelId="model-label"
                        label={uiText("Модель ⓘ")}
                        notched
                        displayEmpty
                        value={model}
                        renderValue={value => models.find(m => m.model === value)?.display_name || value}
                        onChange={(e) => setModel(String(e.target.value))}
                      >
                        {!models.length && <MenuItem disabled value="">{EMPTY_MODELS_TEXT}</MenuItem>}
                        {models.map((item) => (
                          <MenuItem key={item.model} value={item.model} sx={{ whiteSpace: "normal" }}>
                            <Box><Typography>{item.display_name}</Typography>
                              {item.description && <Typography variant="caption" color="text.secondary">{item.description}</Typography>}
                            </Box>
                          </MenuItem>
                        ))}
                      </Select>
                      {modelCatalogReady && providers.length === 0 && user.role !== "admin" && <ModelSetupNotice isAdmin={false} />}
                    </FormControl>

                    {provider === "openai" && (
                      <Stack direction="column" spacing={0.5} sx={{ width: "100%" }}>
                        <Stack direction="row" spacing={1} alignItems="stretch">
                          <TextField
                            fullWidth
                            type="password"
                            label={(
                              <FieldLabelContent
                                label={uiText("API-токен")}
                                hint={apiKey.trim() ? uiText("Используется твой токен из этого поля.") : uiText("Поле пустое — сервер возьмёт ключ из переменной OPENAI_API_KEY.")}
                              />
                            )}
                            InputLabelProps={{ shrink: true }}
                            value={apiKey}
                            onChange={(e) => setApiKey(e.target.value)}
                            placeholder="sk-…"
                          />
                          <Button
                            variant="outlined"
                            onClick={verifyToken}
                            disabled={tokenVerifyLoading}
                            sx={{ whiteSpace: "nowrap", flexShrink: 0 }}
                          >
                            {tokenVerifyLoading ? uiText("Проверяем…") : uiText("Проверить")}
                          </Button>
                        </Stack>
                        {tokenVerifyResult?.ok && (
                          <Typography variant="caption" color="success.main" sx={{ wordBreak: "break-word" }}>
                            {uiText("OK (моделей: {0})", tokenVerifyResult.models.length)}
                          </Typography>
                        )}
                      </Stack>
                    )}
                  </Stack>

                  <Stack direction={{ xs: "column", md: "row" }} spacing={2} sx={{ mt: 2 }} alignItems="stretch">
                    <TextField
                      fullWidth
                      type="number"
                      label={(
                        <FieldLabelContent
                          label={uiText("Лимит отзывов")}
                          hint={uiText("Максимум строк, которые уйдут в модель в этом запуске. Остальные строки файла будут проигнорированы.")}
                        />
                      )}
                      InputLabelProps={{ shrink: true }}
                      value={maxReviewsInput}
                      onChange={(e) => {
                        const next = e.target.value;
                        if (next === "") {
                          setMaxReviewsInput("");
                          return;
                        }
                        if (/^\d+$/.test(next)) {
                          setMaxReviewsInput(next);
                        }
                      }}
                      inputProps={{ min: 1, max: 1000000 }}
                    />

                    <TextField
                      fullWidth
                      type="number"
                      label={(
                        <FieldLabelContent
                          label={uiText("Параллелизм")}
                          hint={uiText("Сколько строк обрабатываются одновременно. Выше — быстрее, но выше нагрузка и риск rate-limit провайдера (макс {0}).", parallelismMax)}
                        />
                      )}
                      InputLabelProps={{ shrink: true }}
                      value={parallelism}
                      onChange={(e) => setParallelism(Math.max(1, Math.min(parallelismMax, Number(e.target.value) || 1)))}
                      inputProps={{ min: 1, max: parallelismMax }}
                    />

                    <TextField
                      fullWidth
                      type="number"
                      label={(
                        <FieldLabelContent
                          label={uiText("Температура")}
                          hint={uiText("0 — строгие задачи, 0.7 — творческие. Для классификации и структурированного анализа используйте 0.")}
                        />
                      )}
                      InputLabelProps={{ shrink: true }}
                      value={temperature}
                      onChange={(e) => setTemperature(Math.max(0, Math.min(2, Number(e.target.value) || 0)))}
                      inputProps={{ min: 0, max: 2, step: 0.1 }}
                    />
                  </Stack>

                  {selectedSheetRows > 0 && (
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 1.5 }}>{uiText("В выбранном листе строк с данными:") + " "}{selectedSheetRows}
                      {groupByColumn && selectedGroupCount !== null && selectedGroupCount !== undefined && (
                        <>
                          {" "}{uiText("· будет обработано") + " "}<b>{selectedGroupCount.toLocaleString(uiLocale())}</b> {pluralizeGroups(selectedGroupCount)}{" " + uiText("(колонка «")}{groupByColumn}»)
                        </>
                      )}
                      {groupByColumn && selectedGroupCount === null && (
                        <>
                          {" "}{uiText("· групп слишком много (более 100 000 уникальных значений) — проверь колонку группировки, возможно она не годится для агрегации")}</>
                      )}
                    </Typography>
                  )}

                  <Stack direction="row" spacing={1} sx={{ mt: 2 }} flexWrap="wrap" useFlexGap>
                    {provider === "openai" && (
                      <Tooltip arrow title={uiText("Токен запомнится в этой вкладке браузера до её закрытия — после F5 вводить заново не нужно. При закрытии вкладки или окна токен пропадает.")}>
                        <Chip
                          label={uiText("Помнить токен на эту вкладку")}
                          color={rememberToken ? "info" : "default"}
                          variant={rememberToken ? "filled" : "outlined"}
                          onClick={() => setRememberToken((v) => !v)}
                        />
                      </Tooltip>
                    )}
                    {provider === "openai" && (
                      <Tooltip arrow title={uiText("Токен шифруется и сохраняется на сервере, чтобы после рестарта сервиса (backend/worker) задачу можно было продолжить без повторного ввода. Нужно только если ставишь свою задачу на длительную обработку.")}>
                        <Chip
                          label={uiText("Помнить токен после перезапуска сервиса")}
                          color={saveApiKeyForResume ? "info" : "default"}
                          variant={saveApiKeyForResume ? "filled" : "outlined"}
                          onClick={() => setSaveApiKeyForResume((v) => !v)}
                        />
                      </Tooltip>
                    )}
                    <Tooltip arrow title={uiText("Если модель уже отвечала на такой же отзыв с тем же промптом — берём сохранённый ответ и не тратим токены. Выключай только для тестов с одинаковыми входными данными и ожиданием разных ответов.")}>
                      <Chip
                        label={uiText("Использовать кэш ответов модели")}
                        color={useCache ? "success" : "default"}
                        variant={useCache ? "filled" : "outlined"}
                        onClick={() => setUseCache((v) => !v)}
                      />
                    </Tooltip>
                  </Stack>
                </AccordionDetails>
              </Accordion>
            )}
          </CardContent>
        </Card>
        )}

        {accessState.has_access && (
        <Card className="card workspace-section" id="prompt-template">
          <CardContent>
            <Stack direction="row" alignItems="center" spacing={1} flexWrap="wrap" useFlexGap>
              <Typography variant="h6" component="h2">{uiText("Шаблон")}</Typography>
                <Button onClick={() => setPromptFullScreen(true)}>{uiText("Полноэкранный редактор")}</Button>
                <Button disabled={previewLoading || !fileInfo} onClick={() => void showPromptPreview()}>{uiText("Показать, что уйдёт в модель")}</Button>
              <Chip
                icon={<InfoOutlinedIcon />}
                label={templateInfoExpanded ? uiText("Скрыть инструкцию") : uiText("Как это работает")}
                size="small"
                color="primary"
                variant={templateInfoExpanded ? "filled" : "outlined"}
                onClick={() => setTemplateInfoExpanded((v) => !v)}
                sx={{ fontWeight: 600, cursor: "pointer" }}
              />
            </Stack>
            <Collapse in={templateInfoExpanded}>
              <Alert severity="info" sx={{ mt: 1 }}>
                <Typography variant="body2" sx={{ fontWeight: 700, mb: 0.5 }}>{uiText("Что такое «Шаблон» — в двух словах")}</Typography>
                <Typography variant="body2">{uiText("Это то, что вы объясняете модели:") + " "}<strong>{uiText("какую задачу решить")}</strong>{" " + uiText("(промпт) и") + " "}<strong>{uiText("в каком виде вернуть ответ")}</strong>{" " + uiText("(поля ответа). Один раз настроили шаблон — дальше сервис прогонит его по всему загруженному файлу, строка за строкой.")}</Typography>

                <Typography variant="body2" sx={{ mt: 1.5, fontWeight: 700 }}>{uiText("Быстрый старт на тестовом файле")}</Typography>
                <Typography variant="body2">{uiText("Если хочется сразу посмотреть как всё работает — в блоке «Данные и модель» справа от зоны загрузки есть кнопка") + " "}<strong>{uiText("«Примеры файлов»")}</strong>{uiText(". В ней лежат готовые xlsx с отзывами клиентов (1 000 и 100 000 строк). Выбираете файл, подбираете под него один из двух готовых пресетов и жмёте запуск. Ниже — какие колонки куда поставить под каждый сценарий.")}</Typography>

                <Typography variant="body2" sx={{ mt: 1, fontWeight: 600 }}>{uiText("Сценарий А. Построчная проверка каждого отзыва")}</Typography>
                <Typography variant="body2">{uiText("Пресет:") + " "}<strong>{uiText("«Пример: Проверка отзывов на мошенничество сотрудников»")}</strong>{uiText(". Модель оценит каждый отзыв по отдельности.")}</Typography>
                <Typography variant="body2">— <em>{uiText("Лист")}</em>{uiText(": первый (единственный в файле).")}</Typography>
                <Typography variant="body2">— <em>{uiText("Колонки для анализа")}</em>: <code>{uiText("отзыв")}</code>{" " + uiText("— единственное поле, которое реально уходит в модель.")}</Typography>
                <Typography variant="body2">— <em>{uiText("Колонки из исходника в итоговый отчёт")}</em>: <code>id</code>, <code>{uiText("магазин")}</code>, <code>{uiText("смена")}</code>, <code>{uiText("оценка")}</code>, <code>{uiText("дата")}</code>{" " + uiText("— попадут в итоговый xlsx без отправки в модель, чтобы потом можно было сопоставить результат с исходной записью.")}</Typography>
                <Typography variant="body2">— <em>{uiText("Группировка")}</em>{uiText(": оставить пустой.")}</Typography>
                <Typography variant="body2">{uiText("— В блоке «Шаблон» в списке «Пресет» выбрать тот же построчный пресет — промпт и поля ответа подставятся сами.")}</Typography>
                <Typography variant="body2">{uiText("— В блоке «3. Обработка» — «Запустить новый анализ». Каждая строка получит категорию, тип нарушения, уверенность и описание.")}</Typography>

                <Typography variant="body2" sx={{ mt: 1, fontWeight: 600 }}>{uiText("Сценарий Б. Сводка по сменам (групповой анализ)")}</Typography>
                <Typography variant="body2">{uiText("Пресет:") + " "}<strong>{uiText("«Пример: Анализ мошенничества по магазинам (группировка)»")}</strong>{uiText(". Модель получит отзывы одной смены целиком и вернёт один агрегат на группу.")}</Typography>
                <Typography variant="body2">— <em>{uiText("Лист")}</em>{uiText(": первый.")}</Typography>
                <Typography variant="body2">— <em>{uiText("Колонки для анализа")}</em>: <code>{uiText("отзыв")}</code>.</Typography>
                <Typography variant="body2">— <em>{uiText("Группировка")}</em>: <code>{uiText("смена")}</code>{" " + uiText("— это главное отличие от сценария А. Модель увидит все отзывы смены разом и оценит её целиком.")}</Typography>
                <Typography variant="body2">— <em>{uiText("Колонки из исходника в итоговый отчёт")}</em>{uiText(": при группировке сохраняются на листе «Исходные данные» с номером группы. Основной лист содержит одну строку на группу.")}</Typography>
                <Typography variant="body2">{uiText("— В блоке «Шаблон» — пресет «Анализ мошенничества по магазинам (группировка)».")}</Typography>
                <Typography variant="body2">{uiText("— В результате каждая смена получит: число подтверждённых нарушений, долю в процентах, встреченные схемы, уровень риска (низкий / средний / высокий) и краткое описание.")}</Typography>

                <Typography variant="body2" sx={{ mt: 0.75, fontStyle: "italic", color: "text.secondary" }}>{uiText("Совет: для обоих сценариев берите файл на 1 000 строк — прогон занимает минуты, можно быстро оценить качество. Файл на 100 000 строк — для нагрузочного теста после того, как шаблон устраивает.")}</Typography>

                <Typography variant="body2" sx={{ mt: 1.5, fontWeight: 700 }}>{uiText("Как сделать под свою задачу")}</Typography>
                <Typography variant="body2" sx={{ mt: 0.5, fontWeight: 600 }}>{uiText("Шаг 1. Напишите промпт")}</Typography>
                <Typography variant="body2">{uiText("Промпт — это текст инструкции для модели. Пишите его как задачу коллеге: короткими и понятными фразами.")}</Typography>
                <Typography variant="body2">{uiText("— Объясните, что модель должна сделать с одной записью из файла (отзывом, заявкой, сообщением).")}</Typography>
                <Typography variant="body2">{uiText("— Опишите критерии. Например: «позитивный» — клиент доволен, «негативный» — есть явная жалоба. Не оставляйте на угадывание.")}</Typography>
                <Typography variant="body2">{uiText("— Если тема сложная — дайте 1–2 коротких примера прямо в промпте.")}</Typography>
                <Typography variant="body2">{uiText("— В промпте") + " "}<strong>{uiText("не нужно")}</strong>{" " + uiText("писать «вот отзыв: …» или подставлять содержимое ячеек вручную. Сервис сам на каждой итерации берёт очередную строку из вашего файла и передаёт её модели вместе с вашим промптом.")}</Typography>

                <Typography variant="body2" sx={{ mt: 0.75, fontWeight: 600 }}>{uiText("Шаг 2. Настройте поля ответа")}</Typography>
                <Typography variant="body2">{uiText("Это «анкета», которую модель обязана заполнить по каждой строке. Называйте поля так, как вам удобно — хоть по-русски.")}</Typography>
                <Typography variant="body2">{uiText("— Для каждого поля укажите тип: текст, число, дата, список-выбор, да/нет, массив.")}</Typography>
                <Typography variant="body2">{uiText("— Если ответ должен быть из фиксированного набора (например, «позитивный / нейтральный / негативный») — используйте тип «список-выбор». Тогда модель не придумает своих категорий.")}</Typography>
                <Typography variant="body2">{uiText("— Для чисел задайте диапазон (например, 1..5), для длинных текстов — ограничение длины, иначе модель напишет целое эссе.")}</Typography>
                <Typography variant="body2">{uiText("— Добавляйте любые поля под свою задачу. Сервис сам проверит ответ модели и, если формат не сошёлся, отправит строку на повторную попытку с объяснением, что исправить.")}</Typography>

                <Typography variant="body2" sx={{ mt: 0.75, fontWeight: 600 }}>{uiText("Шаг 3. Запустите и наблюдайте")}</Typography>
                <Typography variant="body2">{uiText("— Провайдер и модель настраиваются в блоке «Данные и модель» (раскройте «Продвинутые настройки LLM»). По умолчанию уже подставлен рабочий набор — если не хотите менять, переходите дальше.")}</Typography>
                <Typography variant="body2">{uiText("— В блоке «3. Обработка» нажмите «Запустить новый анализ».")}</Typography>
                <Typography variant="body2">{uiText("— В блоке «4. Результаты» прямо во время обработки появляются первые готовые строки. На них удобно сразу увидеть, даёт ли промпт нужный результат, не дожидаясь конца всего отчёта.")}</Typography>
                <Typography variant="body2">{uiText("— Если качество не то — нажмите «Остановить», поправьте промпт или поля, затем снова «Запустить новый анализ». Цена ошибки в начале — секунды, в конце — полчаса.")}</Typography>
                <Typography variant="body2">{uiText("— Готовый отчёт скачивается в xlsx в блоке «4. Результаты». Промежуточные выгрузки доступны и в процессе.")}</Typography>

                <Typography variant="body2" sx={{ mt: 1.5, fontWeight: 700 }}>{uiText("Про модели (практика)")}</Typography>
                <Typography variant="body2">{uiText("Для простых задач (тональность, классификация) подойдёт любая модель. Если в промпте сложные правила с «если…и…иначе…» или числовыми порогами — берите крупные instruct-модели (например, Qwen3-235B, Claude Sonnet/Opus), слабые модели на таких правилах промахиваются.")}</Typography>

                <Typography variant="body2" sx={{ mt: 1, fontWeight: 700 }}>{uiText("Пресеты и свои шаблоны")}</Typography>
                <Typography variant="body2">{uiText("В выпадающем списке «Пресет» сейчас два готовых примера — построчный и групповой анализ мошенничества. Их удобно взять как стартовую точку, даже если ваша задача другая: посмотрите как устроен промпт и поля ответа, замените под себя. Свою версию можно сохранить как личный пресет, чтобы возвращаться к ней в следующих отчётах. Библиотеку готовых пресетов будем расширять.")}</Typography>

              </Alert>
            </Collapse>
            <Stack direction={{ xs: "column", md: "row" }} spacing={1} alignItems={{ xs: "stretch", md: "center" }} sx={{ mt: 1 }}>
              <FormControl sx={{ minWidth: 240, flex: 1 }}>
                <InputLabel id="workspace-preset-label">{uiText("Пресет")}</InputLabel>
                <Select
                  labelId="workspace-preset-label"
                  value={presetPickerValue}
                  label={uiText("Пресет")}
                  onChange={(e) => { void applyPresetSelection(String(e.target.value)); }}
                >
                  {presets.map((preset) => (
                    <MenuItem key={preset.id} value={`user:${preset.id}`}>{uiText("Личный: {0}", preset.name)}</MenuItem>
                  ))}
                  {READY_PRESETS.map((preset) => (
                    <MenuItem key={preset.id} value={`ready:${preset.id}`}>{preset.label}</MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                label={uiText("Имя пресета")}
                value={presetName}
                onChange={(e) => setPresetName(e.target.value)}
                sx={{ minWidth: 200, flex: 1 }}
              />
              <Stack direction="row" spacing={0.5} alignItems="center">
                <Tooltip arrow title={uiText("Сохранить пресет")}>
                  <IconButton color="primary" onClick={savePreset} aria-label={uiText("Сохранить пресет")}>
                    <SaveIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip arrow title={uiText("Загрузить пресет из файла")}>
                  <IconButton color="primary" onClick={() => presetUploadInputRef.current?.click()} aria-label={uiText("Загрузить пресет из файла")}>
                    <CloudUploadIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip arrow title={uiText("Выгрузить пресет в файл")}>
                  <IconButton color="primary" onClick={downloadPreset} aria-label={uiText("Выгрузить пресет в файл")}>
                    <DownloadIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip arrow title={presetPickerValue.startsWith("user:") ? uiText("Удалить пресет") : uiText("Удалить можно только личный пресет")}>
                  <span>
                    <IconButton
                      color="error"
                      onClick={deleteSelectedPreset}
                      disabled={!presetPickerValue.startsWith("user:")}
                      aria-label={uiText("Удалить пресет")}
                    >
                      <DeleteOutlineIcon />
                    </IconButton>
                  </span>
                </Tooltip>
              </Stack>
              <input
                ref={presetUploadInputRef}
                hidden
                type="file"
                accept=".json,.preset.json"
                onChange={(e) => {
                  void importPresetFile(e.target.files?.[0]);
                  e.target.value = "";
                }}
              />
            </Stack>
            <TextField
              multiline
              minRows={6}
              maxRows={24}
              fullWidth
              label={uiText("Промпт")}
              value={promptEditorValue}
              onChange={(e) => setPromptEditorValue(e.target.value)}
              sx={{
                mt: 1.25,
                "& .MuiInputBase-inputMultiline": {
                  resize: "vertical",
                },
              }}
            />
            <Accordion
              expanded={schemaBuilderExpanded}
              onChange={(_, expanded) => setSchemaBuilderExpanded(expanded)}
              sx={{ mt: 2 }}
            >
              <AccordionSummary expandIcon={<ExpandMoreIcon />}>
                <Box>
                  <Typography variant="subtitle1" component="span">{uiText("Настройка полей ответа")}</Typography>
                  <Typography variant="body2" color="text.secondary">{uiText("Здесь настраиваются поля, которые модель должна вернуть по итогам анализа.")}</Typography>
                </Box>
              </AccordionSummary>
              <AccordionDetails>
                <FormControlLabel
                  sx={{ mb: 1 }}
                  control={
                    <Switch
                      checked={showAdvancedSchemaAttrs}
                      onChange={(e) => setShowAdvancedSchemaAttrs(e.target.checked)}
                    />
                  }
                  label={uiText("Расширенные настройки полей (длина, диапазон, формат даты, мин/макс элементов)")}
                />
                <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 1.5 }}>{uiText("Названия полей можно задавать на русском или латинице — модель поймёт любой вариант.")}</Typography>
                <Stack spacing={1.25}>
                  {schemaFields.map((field, index) => (
                    <Card key={field.id} variant="outlined">
                      <CardContent>
                        <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} alignItems={{ md: "center" }}>
                          <TextField
                            label={uiText("Название поля")}
                            placeholder={uiText("Например: тональность, категория, краткое_описание")}
                            value={field.name}
                            onChange={(e) => updateSchemaField(field.id, { name: e.target.value })}
                            fullWidth
                          />
                          <FormControl sx={{ minWidth: 180 }}>
                            <InputLabel id={`output-type-${field.id}`}>{uiText("Тип")}</InputLabel>
                            <Select
                              labelId={`output-type-${field.id}`}
                              value={field.type}
                              label={uiText("Тип")}
                              onChange={(e) => updateSchemaField(field.id, { type: e.target.value as BuilderFieldType })}
                            >
                              <MenuItem value="text">{uiText("Текст")}</MenuItem>
                              <MenuItem value="number">{uiText("Число")}</MenuItem>
                              <MenuItem value="datetime">{uiText("Дата и время")}</MenuItem>
                              <MenuItem value="list">{uiText("Список")}</MenuItem>
                            </Select>
                          </FormControl>
                          <Stack direction="row" spacing={1}>
                            <Button variant="outlined" onClick={() => moveSchemaField(field.id, -1)} disabled={index === 0}>{uiText("Вверх")}</Button>
                            <Button
                              variant="outlined"
                              onClick={() => moveSchemaField(field.id, 1)}
                              disabled={index === schemaFields.length - 1}
                            >{uiText("Вниз")}</Button>
                            <Button variant="outlined" color="error" onClick={() => removeSchemaField(field.id)}>{uiText("Удалить")}</Button>
                          </Stack>
                        </Stack>

                        {field.type === "text" && showAdvancedSchemaAttrs && (
                          <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} sx={{ mt: 1.25 }}>
                            <TextField
                              label={uiText("Мин. длина")}
                              value={field.textMinLength}
                              onChange={(e) => updateSchemaField(field.id, { textMinLength: e.target.value })}
                              sx={{ minWidth: 160 }}
                            />
                            <TextField
                              label={uiText("Макс. длина")}
                              value={field.textMaxLength}
                              onChange={(e) => updateSchemaField(field.id, { textMaxLength: e.target.value })}
                              sx={{ minWidth: 160 }}
                            />
                          </Stack>
                        )}

                        {field.type === "number" && showAdvancedSchemaAttrs && (
                          <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} sx={{ mt: 1.25 }}>
                            <TextField
                              label={uiText("Минимум")}
                              value={field.numberMin}
                              onChange={(e) => updateSchemaField(field.id, { numberMin: e.target.value })}
                              sx={{ minWidth: 180 }}
                            />
                            <TextField
                              label={uiText("Максимум")}
                              value={field.numberMax}
                              onChange={(e) => updateSchemaField(field.id, { numberMax: e.target.value })}
                              sx={{ minWidth: 180 }}
                            />
                            <FormControlLabel
                              control={
                                <Checkbox
                                  checked={field.numberIntegerOnly}
                                  onChange={(e) => updateSchemaField(field.id, { numberIntegerOnly: e.target.checked })}
                                />
                              }
                              label={uiText("Только целое")}
                            />
                          </Stack>
                        )}
                        {field.type === "number" && showAdvancedSchemaAttrs && (
                          <Typography variant="caption" color={
                            [field.numberMin, field.numberMax].some((v) => v.trim() && parseDecimal(v) === null) ? "error" : "text.secondary"
                          } sx={{ display: "block", mt: 0.5 }}>
                            {[field.numberMin, field.numberMax].some((v) => v.trim() && parseDecimal(v) === null)
                              ? uiText("Минимум и максимум должны быть числами, например 0 или 2,5.")
                              : uiText("Дробные значения, например 2,5, можно указывать через запятую. Для оценок с половинками снимите «Только целое».")}
                          </Typography>
                        )}

                        {field.type === "datetime" && showAdvancedSchemaAttrs && (
                          <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} sx={{ mt: 1.25 }}>
                            <FormControl sx={{ minWidth: 220 }}>
                              <InputLabel id={`output-format-${field.id}`}>{uiText("Формат")}</InputLabel>
                              <Select
                                labelId={`output-format-${field.id}`}
                                value={field.datetimeMode}
                                label={uiText("Формат")}
                                onChange={(e) => updateSchemaField(field.id, { datetimeMode: e.target.value as BuilderDateMode })}
                              >
                                <MenuItem value="date">{uiText("Только дата")}</MenuItem>
                                <MenuItem value="datetime">{uiText("Дата и время")}</MenuItem>
                              </Select>
                            </FormControl>
                          </Stack>
                        )}

                        {field.type === "list" && (
                          <Stack spacing={1.25} sx={{ mt: 1.25 }}>
                            <AllowedValuesInput
                              values={field.listValues}
                              onChange={(values) => updateSchemaField(field.id, { listValues: values })}
                            />
                            <FormControlLabel
                              control={
                                <Checkbox
                                  checked={field.listSingle}
                                  onChange={(e) => updateSchemaField(field.id, { listSingle: e.target.checked })}
                                />
                              }
                              label={uiText("Выбрать только одно значение (иначе модель вернёт массив)")}
                            />
                            {!field.listSingle && showAdvancedSchemaAttrs && (
                              <Stack direction={{ xs: "column", md: "row" }} spacing={1.25}>
                                <TextField
                                  label={uiText("Мин. элементов")}
                                  value={field.listMinItems}
                                  onChange={(e) => updateSchemaField(field.id, { listMinItems: e.target.value })}
                                  sx={{ minWidth: 180 }}
                                />
                                <TextField
                                  label={uiText("Макс. элементов")}
                                  value={field.listMaxItems}
                                  onChange={(e) => updateSchemaField(field.id, { listMaxItems: e.target.value })}
                                  sx={{ minWidth: 180 }}
                                />
                              </Stack>
                            )}
                          </Stack>
                        )}
                      </CardContent>
                    </Card>
                  ))}
                </Stack>
                <Stack direction="row" spacing={1} sx={{ mt: 1.25, flexWrap: "wrap" }}>
                  <Button variant="outlined" startIcon={<AddIcon />} onClick={addSchemaField}>{uiText("Добавить поле")}</Button>
                  <Button
                    variant="outlined"
                    onClick={() => {
                      if (validateSchemaBuilder(schemaFields)) {
                        setActionToast({ message: uiText("Схема валидна."), severity: "success" });
                      }
                    }}
                  >{uiText("Проверить схему")}</Button>
                  <Button
                    variant="outlined"
                    startIcon={<RestartAltIcon />}
                    onClick={resetTemplateBuilder}
                  >{uiText("Сброс")}</Button>
                </Stack>
                {schemaBuilderError && (
                  <Alert severity="error" sx={{ mt: 1.25 }}>
                    {schemaBuilderError}
                  </Alert>
                )}
              </AccordionDetails>
            </Accordion>
          </CardContent>
        </Card>
        )}

        {accessState.has_access && (
        <Card className="card workspace-section" id="analysis-run">
          <CardContent>
            <Typography variant="h6" component="h2">{uiText("Обработка")}</Typography>
            <ReportQuota report={selectedLaunchReport} />
            {selectedLaunchReport && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>{uiText("Управление выбранным отчетом:") + " "}{fmtDate(selectedLaunchReport.created_at)}
              </Typography>
            )}
            <Box ref={launchFeedbackRef} tabIndex={-1} sx={{ mt: quotaFailure || launchError || quotaBlocked ? 1.5 : 0, outlineOffset: 4 }}>
              {quotaFailure ? <Alert severity="warning" action={quotaFailure.alternative && <Button color="inherit" onClick={() => {
                setAccessGroupId(quotaFailure.alternative!.id); setQuotaFailure(null); setLaunchError(""); showQuota();
              }}>{uiText("Переключить на «")}{quotaFailure.alternative.name}»</Button>} sx={{ overflowWrap: "anywhere", flexWrap: "wrap", "& .MuiAlert-action": { ml: 0, pl: 0 } }}>
                {quotaFailure.message}
              </Alert> : launchError ? <Alert severity="error">{launchError}</Alert>
                : quotaBlockMessage && <Alert severity="warning" sx={{ overflowWrap: "anywhere" }}
                  action={<Button color="inherit" onClick={showQuota}>{uiText("К квотам")}</Button>}>{quotaBlockMessage}</Alert>}
            </Box>
            <Stack direction="row" spacing={1} sx={{ mt: 1 }} flexWrap="wrap" useFlexGap>
              <Button variant="contained" startIcon={<PlayArrowIcon />} onClick={startAnalysis} disabled={quotaBlocked || !model}>{uiText("Запустить новый анализ")}</Button>
              {!model && <Typography variant="body2" color="text.secondary">{modelCatalogReady && !providers.length
                ? EMPTY_MODELS_TEXT : uiText("Загружаем список моделей…")}</Typography>}
              {(launchStatus === "running" || launchStatus === "queued") && (
                <Button variant="outlined" color="warning" startIcon={<PauseIcon />} onClick={pauseAnalysis}>{uiText("Пауза")}</Button>
              )}
              {launchStatus === "paused" && (
                <Button variant="outlined" color="success" startIcon={<PlayArrowIcon />} onClick={resumeAnalysis}>{uiText("Продолжить")}</Button>
              )}
              <Button
                variant="outlined"
                color="error"
                startIcon={<StopIcon />}
                onClick={cancelAnalysis}
                disabled={!launchJobId || !["running", "queued", "paused"].includes(launchStatus || "")}
              >{uiText("Остановить")}</Button>
              {selectedLaunchReport && (selectedLaunchStatus === "running" || selectedLaunchStatus === "paused") && (
                <Button
                  variant="outlined"
                  startIcon={<DownloadIcon />}
                  onClick={() => openDownloadDialog(selectedLaunchReport, "partial")}
                >{uiText("Скачать промежуточный…")}</Button>
              )}
              {selectedLaunchReport && (selectedLaunchStatus === "failed" || selectedLaunchStatus === "canceled") && (
                <Button
                  variant="outlined"
                  color="secondary"
                  startIcon={<RestartAltIcon />}
                  onClick={() => retryReport(selectedLaunchReport.id, selectedLaunchReport.job_id)}
                  disabled={retryingReportId === selectedLaunchReport.id}
                >{uiText("Перезапустить")}</Button>
              )}
              {selectedLaunchReport && selectedLaunchStatus === "completed" && (Boolean(selectedLaunchReport.results_file) || Boolean(selectedLaunchReport.raw_file) || Boolean(selectedLaunchReport.uploaded_file_id)) && (
                <Button
                  variant="contained"
                  color="success"
                  startIcon={<DownloadIcon />}
                  onClick={() => openDownloadDialog(selectedLaunchReport, "completed")}
                >{uiText("Скачать…")}</Button>
              )}
              {selectedLaunchReport && !["running", "queued", "paused"].includes(selectedLaunchReport.status) && (
                <Button
                  variant="outlined"
                  color="error"
                  startIcon={<DeleteOutlineIcon />}
                  onClick={deleteCurrentFromLaunchBlock}
                >{uiText("Удалить")}</Button>
              )}
            </Stack>

            <Box sx={{ mt: 2 }}>
              {(() => {
                // Единственный источник прогресса в v2.1.0 — snapshot из
                // /api/reports/active (поллинг ~1.5 сек). Для группового
                // режима показываем group_processed/group_total — иначе
                // processed/total.
                const progress = computeReportProgress(selectedLaunchReport);
                const processedDisplay = progress.processed;
                const totalDisplay = progress.total;
                const percentDisplay = totalDisplay > 0
                  ? (processedDisplay / totalDisplay) * 100
                  : 0;
                const etaDisplay = selectedLaunchReport?.eta_seconds ?? null;
                const rawStep = selectedLaunchReport?.current_step || "";
                const filteredStep = visibleCurrentStep(rawStep) || "";
                const stepDisplay = progress.isGrouped && /\d+\s*\/\s*\d+/.test(filteredStep) ? "" : filteredStep;
                const etaLabel = formatEta(etaDisplay);
                const hasAny = Boolean(selectedLaunchReport);
                const isRunningNow = selectedLaunchReport?.status === "running";
                const status = (selectedLaunchReport?.status || "idle") as RingStatus;
                const ringCenterMain = !hasAny
                  ? <span className="ring-percent">—</span>
                  : status === "completed"
                    ? <span className="ring-percent ring-percent-done">100%</span>
                    : status === "failed"
                      ? <span className="ring-percent ring-percent-fail">!</span>
                      : status === "queued"
                        ? <span className="ring-percent ring-percent-queue">…</span>
                        : <AnimatedNumber
                            className="ring-percent"
                            value={percentDisplay}
                            duration={500}
                            format={(n) => `${n.toFixed(1)}%`}
                          />;
                const ringCenterSub = !hasAny
                  ? null
                  : status === "queued"
                    ? <span>{uiText("в очереди")}</span>
                    : <span>{progress.unit}</span>;

                if (!selectedLaunchReport) {
                  return (
                    <Box className="hero-progress hero-progress-empty">
                      <RingProgress percent={0} status="idle" centerLabel={ringCenterMain} />
                      <Box className="hero-progress-meta">
                        <Typography variant="body1" color="text.secondary">{uiText("Ожидание запуска")}</Typography>
                        <Typography variant="caption" color="text.secondary">{uiText("Загрузите файл и нажмите «Запустить новый анализ»")}</Typography>
                      </Box>
                    </Box>
                  );
                }

                return (
                  <>
                    <Box className={`hero-progress hero-progress-${status}`}>
                      <RingProgress
                        percent={percentDisplay}
                        status={status}
                        centerLabel={ringCenterMain}
                        centerSubLabel={ringCenterSub}
                      />
                      <Box className="hero-progress-meta">
                        <Box className="meta-numbers">
                          <AnimatedNumber
                            className="num-current"
                            value={processedDisplay}
                            duration={500}
                          />
                          <span className="num-divider">/</span>
                          <AnimatedNumber
                            className="num-total"
                            value={totalDisplay}
                            duration={500}
                          />
                          <span className="meta-unit">{progress.unit}</span>
                        </Box>
                        <Stack direction="row" spacing={1} alignItems="center" flexWrap="wrap" useFlexGap>
                          <Chip
                            size="small"
                            label={selectedLaunchReport.status_label || statusLabel(selectedLaunchReport.status)}
                            title={selectedLaunchReport.retry_hint || undefined}
                            color={selectedLaunchReport.all_rows_failed ? "error" : selectedLaunchReport.has_errors && selectedLaunchReport.status === "completed" ? "warning" : statusColor(selectedLaunchReport.status)}
                            variant={["running", "completed"].includes(selectedLaunchReport.status) ? "filled" : "outlined"}
                          />
                          {etaLabel && (
                            <Box className="meta-eta">
                              <span className="live-dot" />
                              <span>{etaLabel}</span>
                            </Box>
                          )}
                        </Stack>
                        {stepDisplay && (
                          <Typography variant="caption" color="text.secondary">{stepDisplay}</Typography>
                        )}
                        {progress.isGrouped && <Typography variant="body2">{processedDisplay}{" " + uiText("из") + " "}{totalDisplay}{" " + uiText("групп (")}{percentDisplay.toFixed(1)}%)</Typography>}
                        {selectedLaunchReport.status === "queued"
                          && (selectedLaunchReport.personal_queue
                              || typeof selectedLaunchReport.queue_position === "number") && (
                          <Typography variant="caption" color="text.secondary">
                            {formatQueuedHint(selectedLaunchReport)}
                          </Typography>
                        )}
                      </Box>
                    </Box>

                    <LinearProgress
                      sx={{ mt: 2 }}
                      variant="determinate"
                      value={percentDisplay || 0}
                      color={progressBarColor(selectedLaunchReport.status)}
                      className={selectedLaunchReport.status === "paused" ? "lp-paused" : undefined}
                    />

                    {Array.isArray(selectedLaunchReport.last_logs) && (selectedLaunchReport.last_logs?.length ?? 0) > 0 && (
                      <Box className="logs-feed">
                        {(selectedLaunchReport.last_logs ?? []).slice(-14).map((line, idx) => {
                          const cls = classifyLogLine(line);
                          return (
                            <div key={idx} className={`log-line log-${cls}`}>
                              <span className="log-stripe" />
                              <span className="log-text">{line}</span>
                            </div>
                          );
                        })}
                      </Box>
                    )}

                    {isRunningNow && !etaLabel && (
                      <Typography variant="caption" color="text.secondary" sx={{ mt: 0.5, display: "block" }}>{uiText("В работе")}</Typography>
                    )}
                  </>
                );
              })()}
            </Box>

          </CardContent>
        </Card>
        )}

        {activeResult?.summary && (
          <Box ref={resultsBlockRef} id="analysis-result" className="workspace-section">
          <Card className="card">
              <CardContent>
              <Typography variant="h6" component="h2">{activeResult.title}</Typography>
              <ReportQuota report={selectedLaunchReport} />
              {activeResult.summary.failed_rows > 0 && selectedReportId && <Button sx={{ my: 1 }} variant="outlined"
                href={`${API_ROOT}/${user.role === "admin" ? "admin/" : ""}reports/${selectedReportId}/download/errors/xlsx`}>{uiText("Скачать список ошибок (xlsx)")}</Button>}

              <Stack direction={{ xs: "column", md: "row" }} spacing={2} sx={{ mt: 2 }} alignItems="stretch">
                {selectedLaunchReport?.group_by_column ? (
                  <Card variant="outlined" className="stat-card stat-card-neutral" sx={{ flex: 1, minHeight: 110 }}>
                    <CardContent>
                      <Typography variant="overline" color="text.secondary">{uiText("Всего групп")}</Typography>
                      <Typography component="p" variant="h4">
                        <AnimatedNumber value={selectedLaunchReport.group_processed ?? 0} />
                        <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                          / <AnimatedNumber value={selectedLaunchReport.group_total ?? 0} />
                        </Typography>
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
                        {activeResult.summary.total_rows.toLocaleString(uiLocale())}{" " + uiText("строк · колонка «")}{selectedLaunchReport.group_by_column}»
                      </Typography>
                    </CardContent>
                  </Card>
                ) : (
                  <Card variant="outlined" className="stat-card stat-card-neutral" sx={{ flex: 1, minHeight: 110 }}>
                    <CardContent>
                      <Typography variant="overline" color="text.secondary">{uiText("Всего строк")}</Typography>
                      <Typography component="p" variant="h4"><AnimatedNumber value={activeResult.summary.total_rows} /></Typography>
                    </CardContent>
                  </Card>
                )}
                <Card variant="outlined" className="stat-card stat-card-success" sx={{ flex: 1, minHeight: 110, borderColor: "success.main" }}>
                  <CardContent>
                    <Typography variant="overline" color="success.main">{uiText("Успешно обработано")}</Typography>
                    <Typography component="p" variant="h4">
                      <AnimatedNumber value={activeResult.summary.success_rows} />
                      {activeResult.summary.total_rows > 0 && (
                        <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                          ({((activeResult.summary.success_rows / activeResult.summary.total_rows) * 100).toFixed(1)}%)
                        </Typography>
                      )}
                    </Typography>
                    {Boolean(selectedLaunchReport?.group_by_column) && (
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>{uiText("строк внутри групп")}</Typography>
                    )}
                  </CardContent>
                </Card>
                <Card variant="outlined" className={`stat-card ${activeResult.summary.failed_rows > 0 ? "stat-card-error" : "stat-card-neutral"}`} sx={{ flex: 1, minHeight: 110, borderColor: activeResult.summary.failed_rows > 0 ? "error.main" : undefined }}>
                  <CardContent>
                    <Typography variant="overline" color={activeResult.summary.failed_rows > 0 ? "error.main" : "text.secondary"}>{uiText("С ошибками")}</Typography>
                    <Typography component="p" variant="h4">
                      <AnimatedNumber value={activeResult.summary.failed_rows} />
                      {activeResult.summary.total_rows > 0 && activeResult.summary.failed_rows > 0 && (
                        <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                          ({((activeResult.summary.failed_rows / activeResult.summary.total_rows) * 100).toFixed(1)}%)
                        </Typography>
                      )}
                    </Typography>
                    {Boolean(selectedLaunchReport?.group_by_column) && (
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>{uiText("строк внутри групп")}</Typography>
                    )}
                  </CardContent>
                </Card>
                <Box sx={{ flex: 1, minHeight: 110 }}>
                  <ResponsiveContainer width="100%" height={140}>
                    <PieChart>
                      <Pie data={chartData} dataKey="value" nameKey="name" innerRadius={35} outerRadius={55} />
                      <RechartsTooltip />
                    </PieChart>
                  </ResponsiveContainer>
                </Box>
              </Stack>

              {activeResult.inProgress && (!activeResult.previewRows || activeResult.previewRows.length === 0) && (
                <Alert severity="info" sx={{ mt: 3 }}>{uiText("Первые обработанные строки появятся здесь автоматически. Страница обновляется каждые 5 секунд.")}</Alert>
              )}

              {activeResult.previewRows && activeResult.previewRows.length > 0 && (
                <>
                  <Typography variant="subtitle1" sx={{ mt: 3 }}>
                    {activeResult.inProgress
                      ? uiText("Промежуточный результат — первые {0} обработанных строк", activeResult.previewRows.length)
                      : uiText("Первые {0} обработанных строк", activeResult.previewRows.length)}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 1 }}>
                    {activeResult.inProgress
                      ? uiText("Обновляется каждые 5 секунд. Проверьте качество промпта на лету — если результат неудовлетворителен, можно поставить на паузу и начать новый отчёт.")
                      : uiText("Превью для быстрой проверки качества промпта. Полные данные — в XLSX-выгрузке.")}
                  </Typography>
                  <GrabScrollBox>
                    <Table
                      size="small"
                      sx={{
                        "& td": { verticalAlign: "top" },
                        // Шире в ущерб горизонтальному скроллу: пользователю важнее
                        // читать цельные значения, а не видеть все колонки разом.
                        "& td, & th": { minWidth: 180 },
                      }}
                    >
                      <TableHead>
                        <TableRow>
                          <TableCell>#</TableCell>
                          {previewColumnKeys.map((key) => (
                            <TableCell key={key}>{key}</TableCell>
                          ))}
                          <TableCell>{uiText("Предупреждения")}</TableCell>
                          <TableCell>{uiText("Ошибка")}</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {activeResult.previewRows.map((item) => {
                          const renderValue = (value: unknown): string => {
                            if (value === null || value === undefined) return "-";
                            if (typeof value === "boolean") return value ? uiText("да") : uiText("нет");
                            if (typeof value === "object") return JSON.stringify(value);
                            return String(value);
                          };
                          const clampSx = {
                            // Потолок ширины подняли с 280 до 520 — длинные отзывы
                            // рвались на 5-строчные «бутылочные» ячейки, читать невозможно.
                            maxWidth: 520,
                            maxHeight: "7em",
                            overflow: "hidden",
                            display: "-webkit-box",
                            WebkitLineClamp: 5,
                            WebkitBoxOrient: "vertical" as const,
                            wordBreak: "break-word" as const,
                            whiteSpace: "normal" as const,
                          };
                          return (
                            <TableRow key={item.row_number}>
                              <TableCell>{item.row_number}</TableCell>
                              {previewColumnKeys.map((key) => {
                                const text = renderValue(item.columns?.[key]);
                                return (
                                  <TableCell key={key}>
                                    <Tooltip arrow title={text} placement="top-start">
                                      <Box sx={clampSx}>{text}</Box>
                                    </Tooltip>
                                  </TableCell>
                                );
                              })}
                              <TableCell>
                                <Box sx={clampSx}>
                                  {item.warnings && item.warnings.length > 0 ? item.warnings.join(", ") : "-"}
                                </Box>
                              </TableCell>
                              <TableCell>
                                <Box sx={clampSx}>{item.error || "-"}</Box>
                              </TableCell>
                            </TableRow>
                          );
                        })}
                      </TableBody>
                    </Table>
                  </GrabScrollBox>
                </>
              )}

            </CardContent>
          </Card>
          </Box>
        )}

        {accessState.has_access && (
        <Card className="card workspace-section" id="my-reports">
          <CardContent>
            <Stack direction="row" justifyContent="space-between" alignItems="center">
              <Typography variant="h6" component="h2">{uiText("Мои отчёты")}</Typography>
              <Stack direction="row" spacing={0.5} alignItems="center">
                <Tooltip title={uiText("Обновить")}>
                  <IconButton size="small" onClick={refreshReportsClick} aria-label={uiText("Обновить")}>
                    <RefreshIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
                {reports.length > 0 && <Tooltip title={uiText("Удалить все")}>
                  <span>
                    <IconButton
                      size="small"
                      color="error"
                      onClick={deleteAllReports}
                      disabled={!reports.length || deletingAllReports}
                      aria-label={uiText("Удалить все")}
                    >
                      <DeleteOutlineIcon fontSize="small" />
                    </IconButton>
                  </span>
                </Tooltip>}
              </Stack>
            </Stack>
            {checkedReportIds.length > 0 && <Button color="error" variant="outlined" sx={{ mt: 1 }} disabled={bulkDeleting}
              onClick={() => void deleteCheckedReports()}>{uiText("Удалить выбранные (")}{checkedReportIds.length})</Button>}
            {bulkErrors.map(r => <Alert key={r.id} severity="error" sx={{ mt: 1 }}>{r.id}: {r.message}</Alert>)}
            {visibleReports.length ? (
            <TableContainer sx={{ mt: 1, overflowX: "auto" }}>
            <Table size="small" aria-label={uiText("Мои отчёты")} sx={{ width: "100%", minWidth: reports.length ? 1100 : 0, tableLayout: "fixed" }}>
              <TableHead>
                <TableRow>
                  <TableCell padding="checkbox"><Checkbox inputProps={{ "aria-label": uiText("Выбрать все неактивные отчёты") }}
                    checked={visibleReports.some(r => !["queued", "running", "paused"].includes(r.status)) && visibleReports.filter(r => !["queued", "running", "paused"].includes(r.status)).every(r => checkedReportIds.includes(r.id))}
                    onChange={e => setCheckedReportIds(e.target.checked ? visibleReports.filter(r => !["queued", "running", "paused"].includes(r.status)).map(r => r.id) : [])} /></TableCell>
                  <TableCell sx={{ width: "23%" }}>{uiText("Отчёт")}</TableCell>
                  <TableCell sx={{ width: "16%" }}>{uiText("Создан")}</TableCell>
                  <TableCell sx={{ width: "10%" }}>{uiText("Статус")}</TableCell>
                  <TableCell sx={{ width: "28%" }}>{uiText("Прогресс")}</TableCell>
                  <TableCell sx={{ width: "4%" }} align="center">{uiText("Инфо")}</TableCell>
                  <TableCell sx={{ width: "19%" }} align="right">{uiText("Действия")}</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {visibleReports.map((r) => {
                  const progress = computeReportProgress(r);
                  const { processed: progressProcessed, total: progressTotal, unit: progressUnit } = progress;
                  const progressPercent = Math.round(progress.percent * 10) / 10;
                  const etaLabel = formatEta(r.eta_seconds);
                  const step = visibleCurrentStep(r.current_step);
                  const reportTitle = r.name?.trim() || r.source_filename?.trim() || "—";
                  const hasAnyDownload = Boolean(r.results_file) || Boolean(r.raw_file) || Boolean(r.uploaded_file_id);
                  const canPartialDownload = r.status === "running" || r.status === "paused";
                  const isTerminal = r.status === "completed" || r.status === "failed" || r.status === "canceled";
                  return (
                    <TableRow key={r.id}>
                      <TableCell padding="checkbox"><Checkbox inputProps={{ "aria-label": uiText("Выбрать отчёт {0}", reportTitle) }}
                        disabled={bulkDeleting || !isTerminal} checked={checkedReportIds.includes(r.id)} onChange={e => setCheckedReportIds(prev => e.target.checked ? [...prev, r.id] : prev.filter(id => id !== r.id))} /></TableCell>
                      <TableCell sx={{ wordBreak: "break-word" }}>
                        <Typography variant="body2" fontWeight={500}>{reportTitle}</Typography>
                        <ReportQuota report={r} />
                      </TableCell>
                      <TableCell sx={{ whiteSpace: "nowrap" }}>
                        <Typography variant="caption" color="text.secondary">{fmtDate(r.created_at)}</Typography>
                      </TableCell>
                      <TableCell>
                        <Chip
                          size="small"
                          label={r.status_label || statusLabel(r.status)}
                          title={r.retry_hint || undefined}
                          color={r.all_rows_failed ? "error" : r.has_errors && r.status === "completed" ? "warning" : statusColor(r.status)}
                          variant={r.status === "running" || r.status === "completed" ? "filled" : "outlined"}
                        />
                      </TableCell>
                      <TableCell>
                        <LinearProgress
                          aria-label={uiText("Прогресс отчёта")}
                          variant="determinate"
                          value={Math.max(0, Math.min(100, progressPercent))}
                          color={progressBarColor(r.status)}
                          className={r.status === "paused" ? "lp-paused" : undefined}
                        />
                        <Box sx={{ minHeight: "2.4em", mt: 0.25 }}>
                          {/* Числа на отдельной строке, ETA — на следующей. Так табличный
                              ряд не «прыгает» по высоте при tick'ах счётчика, когда
                              «Осталось ~2 ч 4 мин» сменяется на «~1 ч 59 мин» (разная длина). */}
                          <Typography
                            variant="caption"
                            color="text.secondary"
                            display="block"
                            sx={{ whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                          >
                            {progressProcessed}/{progressTotal} {uiText(progressUnit)} ({progressPercent.toFixed(1)}%)
                          </Typography>
                          {etaLabel && (
                            <Typography
                              variant="caption"
                              color="text.secondary"
                              display="block"
                              sx={{ whiteSpace: "nowrap", overflow: "hidden", textOverflow: "ellipsis" }}
                            >
                              {etaLabel}
                            </Typography>
                          )}
                        </Box>
                        {step && (
                          <Typography variant="caption" color="text.secondary" display="block">
                            {step}
                          </Typography>
                        )}
                        {r.status === "queued" && (r.personal_queue || typeof r.queue_position === "number") && (
                          <Typography variant="caption" color="text.secondary" display="block">
                            {formatQueuedHint(r)}
                          </Typography>
                        )}
                      </TableCell>
                      <TableCell align="center">
                        <Tooltip
                          arrow
                          title={
                            <Box sx={{ fontSize: 12, lineHeight: 1.6, minWidth: 220 }}>
                              <Box sx={{ opacity: 0.7, fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, mb: 0.5 }}>{uiText("Модель и параметры")}</Box>
                              <div>{uiText("Модель:") + " "}<b>{r.provider}/{r.model}</b></div>
                              <div>{uiText("Параллелизм:") + " "}<b>{r.parallelism ?? "-"}</b></div>
                              <div>{uiText("Лимит строк:") + " "}<b>{r.max_reviews ?? "-"}</b></div>
                              <div>{uiText("Температура:") + " "}<b>{r.temperature ?? "-"}</b></div>
                              {r.group_by_column && (<div>{uiText("Группировка:") + " "}<b>{r.group_by_column}</b></div>)}
                              <Box sx={{ borderTop: "1px solid rgba(255,255,255,0.18)", mt: 1, pt: 1 }} />
                              <Box sx={{ opacity: 0.7, fontSize: 11, textTransform: "uppercase", letterSpacing: 0.5, mb: 0.5 }}>{uiText("Использовано токенов")}</Box>
                              <div>{uiText("Запрос:") + " "}<b>{formatTokenExact(r.prompt_tokens)}</b></div>
                              <div>{uiText("Ответ:") + " "}<b>{formatTokenExact(r.completion_tokens)}</b></div>
                              <div>{uiText("Всего:") + " "}<b>{formatTokenExact(r.total_tokens)}</b></div>
                            </Box>
                          }
                        >
                          <InfoOutlinedIcon fontSize="small" color="action" sx={{ cursor: "help" }} />
                        </Tooltip>
                      </TableCell>
                      <TableCell align="right">
                        <Stack direction="row" spacing={0.25} justifyContent="flex-end" alignItems="center">
                          <Tooltip title={uiText("Открыть")}>
                            <span>
                              <IconButton
                                size="small"
                                color="info"
                                onClick={() => openReport(r.id)}
                                disabled={selectedReportLoadingId === r.id}
                                aria-label={uiText("Открыть")}
                              >
                                <OpenInNewIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                          {r.status === "running" && (
                            <Tooltip title={uiText("Пауза")}>
                              <IconButton size="small" color="warning" onClick={() => handleReportAction(r.job_id, "pause")} aria-label={uiText("Пауза")}>
                                <PauseIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {r.status === "paused" && (
                            <Tooltip title={uiText("Продолжить")}>
                              <IconButton size="small" color="success" onClick={() => handleReportAction(r.job_id, "resume")} aria-label={uiText("Продолжить")}>
                                <PlayArrowIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {(r.status === "failed" || r.status === "canceled") && (
                            <Tooltip title={uiText("Перезапустить (кэш включится принудительно)")}>
                              <span>
                                <IconButton
                                  size="small"
                                  color="secondary"
                                  onClick={() => retryReport(r.id, r.job_id)}
                                  disabled={retryingReportId === r.id}
                                  aria-label={uiText("Перезапустить")}
                                >
                                  <RestartAltIcon fontSize="small" />
                                </IconButton>
                              </span>
                            </Tooltip>
                          )}
                          {(r.status === "running" || r.status === "queued" || r.status === "paused") && (
                            <Tooltip title={uiText("Отменить")}>
                              <IconButton size="small" color="error" onClick={() => handleReportAction(r.job_id, "cancel")} aria-label={uiText("Отменить")}>
                                <StopIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {canPartialDownload && (
                            <Tooltip title={uiText("Скачать промежуточный срез")}>
                              <IconButton size="small" color="inherit" onClick={() => openDownloadDialog(r, "partial")} aria-label={uiText("Скачать промежуточный")}>
                                <DownloadIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {hasAnyDownload && isTerminal && (
                            <Tooltip title={uiText("Скачать…")}>
                              <IconButton size="small" color="success" onClick={() => openDownloadDialog(r, "completed")} aria-label={uiText("Скачать")}>
                                <DownloadIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {isTerminal && (
                            <Tooltip title={uiText("Удалить")}>
                              <IconButton size="small" color="error" onClick={() => deleteReport(r.id)} aria-label={uiText("Удалить")}>
                                <DeleteOutlineIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                        </Stack>
                      </TableCell>
                    </TableRow>
                  );
                })}
              </TableBody>
            </Table>
            </TableContainer>
            ) : (
              <Typography color="text.secondary" variant="body2" sx={{ mt: 1 }}>{uiText("Пока нет отчётов")}</Typography>
            )}
            {reports.length > 3 && (
              <Box sx={{ display: "flex", justifyContent: "center", mt: 1.25 }}>
                <Button
                  size="small"
                  variant="text"
                  onClick={() => setShowAllReports((v) => !v)}
                  endIcon={showAllReports ? <ExpandLessIcon /> : <ExpandMoreIcon />}
                  sx={{
                    textTransform: "none",
                    color: "text.secondary",
                    fontWeight: 500,
                    px: 2,
                    py: 0.5,
                    borderRadius: 999,
                    transition: "background-color 180ms ease, color 180ms ease",
                    "&:hover": {
                      backgroundColor: (theme) =>
                        theme.palette.mode === "dark"
                          ? "rgba(255,255,255,0.06)"
                          : "rgba(15,23,42,0.04)",
                      color: "text.primary",
                    },
                  }}
                >
                  {showAllReports ? uiText("Свернуть") : uiText("Показать ещё {0}", reports.length - 3)}
                </Button>
              </Box>
            )}
          </CardContent>
        </Card>
        )}

      </Box>
        {releaseDialog}
        {rulesDialog}
        <Drawer
          anchor="right"
          open={examplesOpen}
          onClose={() => setExamplesOpen(false)}
          PaperProps={{ sx: { width: { xs: "100%", sm: 380 }, p: 2 } }}
        >
          <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 1.5 }}>
            <Typography variant="h6">{uiText("Примеры файлов")}</Typography>
            <IconButton size="small" onClick={() => setExamplesOpen(false)} aria-label={uiText("Закрыть")}>
              <CloseIcon fontSize="small" />
            </IconButton>
          </Stack>
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 2 }}>{uiText("Клик по карточке — файл подставится как обычно загруженный, дальше настраиваете шаблон и запускаете.")}</Typography>
          {examplesLoading && (
            <Stack alignItems="center" sx={{ py: 3 }}>
              <LinearProgress aria-label={uiText("Загрузка примеров")} sx={{ width: "100%" }} />
            </Stack>
          )}
          {!examplesLoading && examples.length === 0 && (
            <Typography variant="body2" color="text.secondary">{uiText("В директории `examples/` нет доступных файлов.")}</Typography>
          )}
          <Stack spacing={1.5}>
            {examples.map((item) => (
              <Card key={item.name} variant="outlined">
                <CardContent sx={{ pb: 1 }}>
                  <Typography variant="subtitle2" sx={{ wordBreak: "break-word" }}>
                    {item.name}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">{uiText("Размер:") + " "}{formatBytes(item.size_bytes)}
                  </Typography>
                </CardContent>
                <Stack direction="row" justifyContent="flex-end" sx={{ px: 2, pb: 1.5 }}>
                  <Button
                    size="small"
                    variant="contained"
                    onClick={() => useExample(item.name)}
                    disabled={exampleLoadingName !== null}
                  >
                    {exampleLoadingName === item.name ? uiText("Загрузка…") : uiText("Использовать")}
                  </Button>
                </Stack>
              </Card>
            ))}
          </Stack>
        </Drawer>
        <Dialog fullScreen open={promptFullScreen} onClose={() => setPromptFullScreen(false)}>
          <DialogTitle>{uiText("Редактор промпта")}</DialogTitle><DialogContent>
            <TextField fullWidth multiline label={uiText("Промпт")} value={promptEditorValue} onChange={e => setPromptEditorValue(e.target.value)}
              sx={{ mt: 1, "& textarea": { minHeight: "60vh", fontFamily: "monospace" } }} />
          </DialogContent><DialogActions><Button variant="contained" onClick={() => setPromptFullScreen(false)}>{uiText("Сохранить и закрыть")}</Button></DialogActions>
        </Dialog>
        <Dialog open={!!pendingSettingsPayload} onClose={() => setPendingSettingsPayload(null)} maxWidth="sm" fullWidth>
          <DialogTitle>{uiText("Проверьте колонки перед запуском")}</DialogTitle><DialogContent>
            <Typography>{uiText("Поля ответа совпадают с колонками источника, которые не передаются в модель или результат:")}</Typography>
            <Typography sx={{ my: 2, fontWeight: 600 }}>{settingsWarnings.join(", ")}</Typography>
            {groupByColumn && <Alert severity="info">{uiText("Справочные колонки будут сохранены без потерь на листе «Исходные данные» с номером группы. В модель их значения не отправляются.")}</Alert>}
          </DialogContent><DialogActions sx={{ flexDirection: "column", alignItems: "stretch", gap: 1 }}>
            <Button variant="contained" onClick={() => void resolveSettings("passthrough")}>{uiText("Передать колонку в результат как есть (рекомендуется)")}</Button>
            <Button onClick={() => void resolveSettings("analysis")}>{uiText("Включить в анализ")}</Button>
            <Button onClick={() => void resolveSettings("keep")}>{uiText("Запустить как есть")}</Button>
            <Button color="inherit" onClick={() => setPendingSettingsPayload(null)}>{uiText("Отмена")}</Button>
          </DialogActions>
        </Dialog>
        <Dialog open={!!promptPreview} onClose={() => setPromptPreview(null)} maxWidth="lg" fullWidth>
          <DialogTitle>{uiText("Что уйдёт в модель")}</DialogTitle><DialogContent>
            <Alert severity="info">{uiText("Предпросмотр без вызова модели и расхода токенов.")}{promptPreview?.truncated ? " " + uiText("Длинные сообщения сокращены до 50 000 символов.") : ""}{promptPreview && !promptPreview.will_send ? " " + uiText("Первая строка пустая и будет пропущена.") : ""}</Alert>
            {promptPreview?.messages.map((message, index) => <Box key={index} sx={{ mt: 2 }}><Typography variant="subtitle1">{message.role}</Typography>
              <Box component="pre" sx={{ whiteSpace: "pre-wrap", overflowWrap: "anywhere", fontSize: 13, maxHeight: "35vh", overflow: "auto" }}>{message.content}</Box></Box>)}
            <Typography variant="subtitle1">{uiText("Схема ответа")}</Typography><Box component="pre" sx={{ whiteSpace: "pre-wrap" }}>{JSON.stringify(promptPreview?.response_schema, null, 2)}</Box>
          </DialogContent><DialogActions><Button onClick={() => setPromptPreview(null)}>{uiText("Закрыть")}</Button></DialogActions>
        </Dialog>
        <Dialog open={Boolean(downloadDialog)} onClose={() => setDownloadDialog(null)} maxWidth="sm" fullWidth>
          <DialogTitle>{uiText("Скачать отчёт")}</DialogTitle>
          <DialogContent>
            {downloadDialog && (
              <Stack spacing={2} sx={{ mt: 1 }}>
                {(downloadDialog.status === "running" || downloadDialog.status === "paused") && (
                  <PartialDownloadHint />
                )}
                <FormControl>
                  <Typography variant="subtitle2" sx={{ mb: 1 }}>{uiText("Формат")}</Typography>
                  <RadioGroup
                    value={downloadDialog.format}
                    onChange={(_, value) => setDownloadDialog((prev) => prev ? { ...prev, format: value as "xlsx" | "raw" | "source" } : prev)}
                  >
                    {downloadDialog.hasResults && (
                      <FormControlLabel value="xlsx" control={<Radio />} label={uiText("XLSX (таблица с результатом)")} />
                    )}
                    {downloadDialog.hasRaw && (
                      <FormControlLabel value="raw" control={<Radio />} label={uiText("Сырой JSON (ответы модели + метаданные)")} />
                    )}
                    {downloadDialog.hasSource && (
                      <FormControlLabel value="source" control={<Radio />} label={uiText("Исходный файл пользователя")} />
                    )}
                  </RadioGroup>
                </FormControl>
                <TextField
                  label={uiText("Имя файла")}
                  value={downloadDialog.filename}
                  onChange={(e) => setDownloadDialog((prev) => prev ? { ...prev, filename: e.target.value } : prev)}
                  helperText={uiText("Без расширения — оно добавится автоматически по выбранному формату. Недопустимые символы заменятся на _.")}
                  fullWidth
                />
              </Stack>
            )}
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setDownloadDialog(null)}>{uiText("Отмена")}</Button>
            <Button variant="contained" startIcon={<DownloadIcon />} onClick={submitDownload}>{uiText("Скачать")}</Button>
          </DialogActions>
        </Dialog>

        <Snackbar
          open={Boolean(error)}
          autoHideDuration={6000}
          onClose={handleErrorClose}
          anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
        >
          <Alert severity="error" variant="filled" onClose={handleErrorClose} sx={{ width: "100%" }}>
            {error}
          </Alert>
        </Snackbar>
        <Snackbar
          open={Boolean(actionToast)}
          autoHideDuration={2500}
          onClose={handleSuccessClose}
          anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
        >
          <Alert severity={actionToast?.severity || "success"} variant="filled" onClose={handleSuccessClose} sx={{ width: "100%" }}>
            {actionToast?.message || ""}
          </Alert>
        </Snackbar>
    </Box>
  );
}

export default App;


