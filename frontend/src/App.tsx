import { useEffect, useMemo, useRef, useState, type ClipboardEvent } from "react";
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
import MoreVertIcon from "@mui/icons-material/MoreVert";
import MoreHorizIcon from "@mui/icons-material/MoreHoriz";
import OpenInNewIcon from "@mui/icons-material/OpenInNew";
import InfoOutlinedIcon from "@mui/icons-material/InfoOutlined";
import RefreshIcon from "@mui/icons-material/Refresh";
import { Pie, PieChart, ResponsiveContainer, Tooltip as RechartsTooltip } from "recharts";

import { api, type ExampleFile, type ReleaseEntry } from "./api";
import ReactMarkdown from "react-markdown";
import { GrabScrollBox } from "./useGrabScroll";
import type {
  AdminLogItem,
  AdminStats,
  AdminUserItem,
  FileInspectResponse,
  JobState,
  JobSummary,
  Provider,
  ReportAnalysis,
  ReportItem,
  SseEvent,
  Usage,
  User,
  UserPreset,
  VerifyTokenResult,
} from "./types";
import "./styles.css";
import { useThemeMode } from "./theme";

const TOKEN_SESSION_KEY = "review_analyzer_provider_token";
const LAST_PROVIDER_KEY = "review_analyzer_last_provider";
const LAST_MODEL_KEY = "review_analyzer_last_model";
const APP_VERSION = "2.0.0";

function formatQueueHint(position: number | null | undefined): string {
  if (typeof position !== "number" || position < 0) return "";
  if (position === 0) return "Следующий в очереди";
  return `Перед вами: ${position}`;
}
const BRAND_LOGO_CANDIDATES = [
  "/branding/logo.png",
  "/branding/logo-default.png",
  "/logo-default.svg",
] as const;
type ReadyPresetId = "fraud_individual" | "fraud_by_store";
const READY_PRESET_IDS: ReadyPresetId[] = ["fraud_individual", "fraud_by_store"];
const READY_PRESETS: Array<{ id: ReadyPresetId; label: string }> = [
  { id: "fraud_individual", label: "Пример: Проверка отзывов на мошенничество сотрудников" },
  { id: "fraud_by_store", label: "Пример: Анализ мошенничества по магазинам (группировка)" },
];
const API_ROOT = api.base.endsWith("/api") ? api.base : `${api.base}/api`;

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
  return d.toLocaleString("ru-RU");
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
    case "queued": return "В очереди";
    case "running": return "В работе";
    case "paused": return "На паузе";
    case "completed": return "Готов";
    case "failed": return "Ошибка";
    case "canceled": return "Отменён";
    default: return status;
  }
}

function statusColor(status: string): "default" | "primary" | "success" | "warning" | "error" | "info" {
  switch (status) {
    case "running": return "warning";    // активное действие — оранжевый привлекает внимание
    case "completed": return "success";  // зелёный — готово
    case "paused": return "default";     // спокойный серый — на паузе
    case "failed": return "error";       // красный — ошибка
    case "canceled": return "default";   // серый — отменён пользователем
    case "queued": return "info";        // голубой — нейтральное ожидание
    default: return "default";
  }
}

function progressBarColor(status: string): "primary" | "success" | "warning" | "error" | "info" | "inherit" {
  // LinearProgress не поддерживает "default", поэтому для нейтральных статусов
  // отдаём "inherit" (наследует цвет текста — серовато-нейтрально).
  switch (status) {
    case "running": return "warning";
    case "completed": return "success";
    case "failed": return "error";
    case "queued": return "info";
    case "paused":
    case "canceled":
    default:
      return "inherit";
  }
}

function pluralizeGroups(n: number): string {
  const mod100 = n % 100;
  const mod10 = n % 10;
  if (mod100 >= 11 && mod100 <= 14) return "групп";
  if (mod10 === 1) return "группа";
  if (mod10 >= 2 && mod10 <= 4) return "группы";
  return "групп";
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
  if (s < 60) return `Осталось ~${s} сек`;
  if (s < 3600) return `Осталось ~${Math.round(s / 60)} мин`;
  const hours = Math.floor(s / 3600);
  const minutes = Math.round((s % 3600) / 60);
  return minutes > 0 ? `Осталось ~${hours} ч ${minutes} мин` : `Осталось ~${hours} ч`;
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
  return trimmed;
}

function createFieldId(): string {
  if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
    return crypto.randomUUID();
  }
  return `field_${Math.random().toString(36).slice(2, 10)}`;
}

function parseLines(value: string): string[] {
  return value
    .split(/\r?\n|,/)
    .map((item) => item.trim())
    .filter(Boolean);
}

function buildLines(values: string[]): string {
  return values.join("\n");
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
    if (field.numberMin.trim()) schema.min = Number(field.numberMin);
    if (field.numberMax.trim()) schema.max = Number(field.numberMax);
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
      throw new Error(`Поле '${name}' не является объектом схемы.`);
    }
    ordered.push(schemaToField(name, rawSchema as Record<string, unknown>));
  }
  return ordered;
}

function validateExpectedJsonTemplate(payload: Record<string, unknown>): string | null {
  const keys = Object.keys(payload);
  if (!keys.length) return "Ожидаемый JSON не должен быть пустым — добавьте хотя бы одно поле.";
  const validateFieldSchema = (fieldName: string, value: unknown): string | null => {
    if (!value || Array.isArray(value) || typeof value !== "object") {
      return `Поле '${fieldName}' должно быть объектом схемы, например {"type":"string"}.`;
    }
    const schema = value as Record<string, unknown>;
    const schemaType = typeof schema.type === "string" ? schema.type.trim().toLowerCase() : "";
    if (!schemaType) return `Поле '${fieldName}' должно содержать ключ 'type'.`;

    if (schemaType === "string") {
      if ("min_length" in schema && (!Number.isInteger(schema.min_length) || Number(schema.min_length) < 0)) {
        return `Поле '${fieldName}': min_length должен быть целым числом >= 0.`;
      }
      if ("max_length" in schema && (!Number.isInteger(schema.max_length) || Number(schema.max_length) < 1)) {
        return `Поле '${fieldName}': max_length должен быть целым числом >= 1.`;
      }
      return null;
    }

    if (schemaType === "date" || schemaType === "datetime") {
      return null;
    }

    if (schemaType === "number" || schemaType === "integer") {
      if ("min" in schema && typeof schema.min !== "number") {
        return `Поле '${fieldName}': min должен быть числом.`;
      }
      if ("max" in schema && typeof schema.max !== "number") {
        return `Поле '${fieldName}': max должен быть числом.`;
      }
      return null;
    }

    if (schemaType === "boolean") {
      return null;
    }

    if (schemaType === "enum") {
      if (!Array.isArray(schema.values) || !schema.values.length) {
        return `Поле '${fieldName}' с type=enum должно содержать непустой массив values.`;
      }
      if (schema.values.some((item) => typeof item !== "string" || !item.trim())) {
        return `Поле '${fieldName}' с type=enum должно содержать только непустые строки в values.`;
      }
      return null;
    }

    if (schemaType === "array") {
      if (!schema.items || Array.isArray(schema.items) || typeof schema.items !== "object") {
        return `Поле '${fieldName}' с type=array должно содержать объект items.`;
      }
      return validateFieldSchema(`${fieldName}[]`, schema.items);
    }

    if (schemaType === "object") {
      if (!("properties" in schema)) return null;
      if (!schema.properties || Array.isArray(schema.properties) || typeof schema.properties !== "object") {
        return `Поле '${fieldName}' с type=object должно содержать объект properties.`;
      }
      const properties = schema.properties as Record<string, unknown>;
      for (const [childKey, childValue] of Object.entries(properties)) {
        if (!childKey.trim()) return `Поле '${fieldName}' содержит пустой ключ в properties.`;
        const nestedError = validateFieldSchema(`${fieldName}.${childKey}`, childValue);
        if (nestedError) return nestedError;
      }
      if ("required" in schema) {
        if (!Array.isArray(schema.required) || schema.required.some((item) => typeof item !== "string")) {
          return `Поле '${fieldName}': required должен быть массивом строк.`;
        }
      }
      return null;
    }

    return `Поле '${fieldName}': неподдерживаемый type='${schemaType}'.`;
  };

  for (const key of keys) {
    if (!key.trim()) return "В Ожидаемом JSON есть пустой ключ.";
    const error = validateFieldSchema(key, payload[key]);
    if (error) return error;
  }
  return null;
}

function fallbackReadyPreset(
  hint: ReadyPresetId,
): { prompt: string; expected: Record<string, unknown> } {
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
          label="Допустимые значения"
          placeholder="Введите значение и нажмите Enter. Можно вставить колонку из Excel."
          helperText="Enter — добавить. Ctrl+V из Excel или списка через запятую/точку с запятой — каждое значение станет отдельным чипом."
          onPaste={handlePaste}
        />
      )}
    />
  );
}

function App() {
  const [providers, setProviders] = useState<Provider[]>([]);
  const [models, setModels] = useState<string[]>([]);
  const [defaultPrompt, setDefaultPrompt] = useState("");

  const [user, setUser] = useState<User | null>(null);
  const [authMode, setAuthMode] = useState<"login" | "register">("login");
  const [authUsername, setAuthUsername] = useState("");
  const [authPassword, setAuthPassword] = useState("");
  const [authLoading, setAuthLoading] = useState(false);

  const [reports, setReports] = useState<ReportItem[]>([]);
  const [adminUsers, setAdminUsers] = useState<AdminUserItem[]>([]);
  const [adminSelectedUserId, setAdminSelectedUserId] = useState<number | "">("");
  const [adminReports, setAdminReports] = useState<ReportItem[]>([]);
  const [adminStats, setAdminStats] = useState<AdminStats | null>(null);
  const [adminLoading, setAdminLoading] = useState(false);
  const [adminAutoRefresh, setAdminAutoRefresh] = useState(false);
  const [adminLogService, setAdminLogService] = useState<"all" | "backend" | "worker">("all");
  const [adminLogLevel, setAdminLogLevel] = useState<"" | "INFO" | "WARNING" | "ERROR">("");
  const [adminLogQuery, setAdminLogQuery] = useState("");
  const [adminLogLinesLimit, setAdminLogLinesLimit] = useState(200);
  const [adminLogs, setAdminLogs] = useState<AdminLogItem[]>([]);
  const [adminLogsLoading, setAdminLogsLoading] = useState(false);
  const [adminLogsAutoRefresh, setAdminLogsAutoRefresh] = useState(false);
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
  const [releaseNotesOpen, setReleaseNotesOpen] = useState(false);
  const [releases, setReleases] = useState<ReleaseEntry[]>([]);
  const [releasesLoading, setReleasesLoading] = useState(false);
  const [releasesError, setReleasesError] = useState<string | null>(null);
  const [activeReleaseTab, setActiveReleaseTab] = useState(0);
  const [sheetName, setSheetName] = useState("");
  const [analysisColumns, setAnalysisColumns] = useState<string[]>([]);
  const [nonAnalysisColumns, setNonAnalysisColumns] = useState<string[]>([]);
  const [groupByColumn, setGroupByColumn] = useState("");

  const [provider, setProvider] = useState(() => localStorage.getItem(LAST_PROVIDER_KEY) || "openai");
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

  const [jobId, setJobId] = useState<string | null>(null);
  const [job, setJob] = useState<JobState | null>(null);
  const [, setStatusLogs] = useState<string[]>([]);
  const [jobElapsedSec, setJobElapsedSec] = useState(0);
  const [selectedReportId, setSelectedReportId] = useState<string | null>(null);
  const [selectedReportAnalysis, setSelectedReportAnalysis] = useState<ReportAnalysis | null>(null);
  const [selectedReportLoading, setSelectedReportLoading] = useState(false);
  const [selectedReportLoadingId, setSelectedReportLoadingId] = useState<string | null>(null);
  const resultsBlockRef = useRef<HTMLDivElement | null>(null);
  const scrolledReportIdRef = useRef<string | null>(null);
  const fileInputRef = useRef<HTMLInputElement | null>(null);
  const eventSourceRef = useRef<EventSource | null>(null);
  const statusPollRef = useRef<number | null>(null);
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
  const [reportsMenuAnchor, setReportsMenuAnchor] = useState<HTMLElement | null>(null);
  const [isRunning, setIsRunning] = useState(false);
  const [logoSrc, setLogoSrc] = useState<string>(BRAND_LOGO_CANDIDATES[0]);
  const { mode, toggle } = useThemeMode();
  const JOB_REPORTS_POLL_ACTIVE_MS = 3000;
  const JOB_REPORTS_POLL_IDLE_MS = 6000;
  const JOB_STATUS_FALLBACK_POLL_MS = 3000;
  const USAGE_POLL_MS = 20000;
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
        setSchemaBuilderError("У каждого поля должно быть заполнено название.");
        return false;
      }
      if (names.has(trimmed)) {
        setSchemaBuilderError(`Названия полей должны быть уникальны. Повтор: '${trimmed}'.`);
        return false;
      }
      names.add(trimmed);
      if (field.type === "list" && field.listSingle && field.listValues.length === 0) {
        setSchemaBuilderError(
          `Поле '${trimmed}': для списка с одним значением нужно заполнить допустимые значения.`,
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

  function nowTsLabel(): string {
    return new Date().toLocaleTimeString("ru-RU", { hour12: false });
  }

  function stopJobTracking() {
    if (eventSourceRef.current) {
      eventSourceRef.current.close();
      eventSourceRef.current = null;
    }
    if (statusPollRef.current !== null) {
      window.clearInterval(statusPollRef.current);
      statusPollRef.current = null;
    }
  }

  async function verifyToken() {
    if (provider !== "openai") return;
    setTokenVerifyLoading(true);
    setTokenVerifyResult(null);
    try {
      const result = await api.verifyProviderToken(provider, apiKey.trim() ? apiKey : null);
      setTokenVerifyResult(result);
      if (!result.ok) {
        setError(result.message || `Проверка токена: HTTP ${result.status_code}`);
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
    if (providerList.length > 0) {
      const available = new Set(providerList.map((item) => item.id));
      if (!available.has(provider)) {
        const preferred = providerList.find((item) => item.id === "openai")?.id || providerList[0]?.id || "";
        if (preferred) {
          setProvider(preferred);
        }
      }
    }
    setDefaultPrompt(promptData.promptTemplate);
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
      let shouldReloadFull = false;

      setReports((prev) => {
        const prevActiveIds = prev
          .filter((row) => row.status === "running" || row.status === "queued" || row.status === "paused")
          .map((row) => row.id);
        shouldReloadFull = prevActiveIds.some((id) => !activeIds.has(id));

        const next = prev.map((row) => {
          const fresh = activeById.get(row.id);
          if (!fresh) return row;
          return {
            ...row,
            ...fresh,
            // Keep heavy JSON fields from the full list endpoint.
            summary_json: row.summary_json,
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
    try {
      const data = await api.usage();
      setUsage(data);
    } catch {
      // no-op
    }
  }

  async function loadAdminOverview() {
    setAdminLoading(true);
    try {
      const [usersData, statsData] = await Promise.all([api.adminUsers(), api.adminStats()]);
      setAdminUsers(usersData);
      setAdminStats(statsData);
      setAdminSelectedUserId((prev) => {
        if (typeof prev === "number" && usersData.some((u) => u.id === prev)) return prev;
        return usersData[0]?.id ?? "";
      });
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setAdminLoading(false);
    }
  }

  async function loadAdminReports(targetUserId: number) {
    setAdminLoading(true);
    try {
      const rows = await api.adminUserReports(targetUserId, 50);
      setAdminReports(rows);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setAdminLoading(false);
    }
  }

  async function loadAdminLogs() {
    setAdminLogsLoading(true);
    try {
      const lines = await api.adminLogs(adminLogService, adminLogLinesLimit, adminLogLevel || undefined, adminLogQuery || undefined);
      setAdminLogs(lines);
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setAdminLogsLoading(false);
    }
  }

  useEffect(() => {
    async function bootstrap() {
      try {
        await loadBaseData();
      } catch (e) {
        setError((e as Error).message);
        return;
      }

      try {
        const me = await api.me();
        setUser(me);
        await loadReports();
        await loadUsage();
        await loadPresets();
        if (me.role === "admin") {
          await loadAdminOverview();
          await loadAdminLogs();
        }
      } catch {
        // Not authenticated yet is an expected state on fresh load.
      }
    }

    bootstrap();
    return () => {
      stopJobTracking();
    };
  }, []);

  useEffect(() => {
    if (!isRunning) {
      setJobElapsedSec(0);
      return;
    }
    const startedAt = Date.now();
    const timer = window.setInterval(() => {
      setJobElapsedSec(Math.floor((Date.now() - startedAt) / 1000));
    }, 1000);
    return () => window.clearInterval(timer);
  }, [isRunning, jobId]);

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
    if (user?.role !== "admin") return;
    if (typeof adminSelectedUserId !== "number") {
      setAdminReports([]);
      return;
    }
    loadAdminReports(adminSelectedUserId);
  }, [adminSelectedUserId, user?.role]);

  useEffect(() => {
    if (user?.role !== "admin" || !adminAutoRefresh) return;
    const timer = window.setInterval(() => {
      loadAdminOverview();
      if (typeof adminSelectedUserId === "number") {
        loadAdminReports(adminSelectedUserId);
      }
    }, 5000);
    return () => window.clearInterval(timer);
  }, [adminAutoRefresh, adminSelectedUserId, user?.role]);

  useEffect(() => {
    if (user?.role !== "admin" || !adminLogsAutoRefresh) return;
    const timer = window.setInterval(() => {
      loadAdminLogs();
    }, 3000);
    return () => window.clearInterval(timer);
  }, [adminLogsAutoRefresh, adminLogService, adminLogLevel, adminLogQuery, adminLogLinesLimit, user?.role]);

  useEffect(() => {
    async function loadModels() {
      try {
        const modelsList = await api.getModels(provider);
        setModels(modelsList);
        const saved = localStorage.getItem(LAST_MODEL_KEY);
        if (saved && modelsList.includes(saved)) {
          setModel(saved);
        } else {
          setModel(modelsList[0] || "");
        }
      } catch (e) {
        setError((e as Error).message);
      }
    }

    loadModels();
  }, [provider]);

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

  const sortedAdminLogs = useMemo(() => {
    const withIndex = adminLogs.map((item, idx) => ({ item, idx }));
    withIndex.sort((a, b) => {
      const ta = Date.parse(a.item.ts || "");
      const tb = Date.parse(b.item.ts || "");
      const aValid = Number.isFinite(ta);
      const bValid = Number.isFinite(tb);
      if (aValid && bValid) return tb - ta; // newest first
      if (aValid) return -1;
      if (bValid) return 1;
      return b.idx - a.idx;
    });
    return withIndex.map((x) => x.item);
  }, [adminLogs]);

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

  async function doAuth() {
    setAuthLoading(true);
    setError("");
    try {
      await (authMode === "login"
        ? await api.login(authUsername, authPassword)
        : await api.register(authUsername, authPassword));
      const me = await api.me();
      setUser(me);
      await loadReports();
      await loadUsage();
      await loadPresets();
      if (me.role === "admin") {
        await loadAdminOverview();
        await loadAdminLogs();
      }
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setAuthLoading(false);
    }
  }

  async function doLogout() {
    try {
      await api.logout();
    } catch {
      // ignore
    }
    setUser(null);
    setReports([]);
    setAdminUsers([]);
    setAdminReports([]);
    setAdminStats(null);
    setAdminSelectedUserId("");
    setAdminLogs([]);
    setAdminAutoRefresh(false);
    setAdminLogsAutoRefresh(false);
    setPresets([]);
    setJob(null);
    setJobId(null);
    setSelectedReportId(null);
    setSelectedReportAnalysis(null);
    setStatusLogs([]);
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
      setReleasesError(err instanceof Error ? err.message : String(err));
    } finally {
      setReleasesLoading(false);
    }
  }

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
    setError("");
    setFileInspectStatus("queued");
    setFileInspectMessage("Файл загружен. Подготавливаем структуру Excel...");
    try {
      const applyReadyInfo = (info: FileInspectResponse) => {
        setFileInfo(info);
        setFileInspectStatus("ready");
        setFileInspectMessage("Файл готов. Выберите колонки и запустите анализ.");
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
          setFileInspectMessage("Читаем Excel и определяем листы/колонки...");
        } else if (status === "queued") {
          setFileInspectStatus("queued");
          setFileInspectMessage("Файл в очереди на подготовку...");
        }
        if (current.inspect_status === "error") {
          throw new Error(current.inspect_error_text || "Ошибка подготовки файла.");
        }
        if (current.inspect_status === "ready") {
          applyReadyInfo(current);
          return;
        }
      }
      throw new Error("Подготовка файла заняла слишком много времени. Попробуйте позже.");
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
    // При группировке справочные колонки недоступны: значения внутри группы
    // могут различаться, агрегировать нечем — сбрасываем список целиком.
    setNonAnalysisColumns([]);
  }, [groupByColumn]);

  function onDrop(event: React.DragEvent<HTMLDivElement>) {
    event.preventDefault();
    setDragActive(false);
    const file = event.dataTransfer.files?.[0];
    handleFileUpload(file);
  }

  async function startAnalysis() {
    if (!fileInfo) {
      setError("Сначала загрузите файл Excel.");
      return;
    }
    const promptTemplate = promptEditorValue.trim();
    const expectedJsonTemplate = expectedJsonObject;

    if (!promptTemplate) {
      setError("Промпт не должен быть пустым.");
      return;
    }
    if (!validateSchemaBuilder(schemaFields)) return;

    const effectiveAnalysisColumns = analysisColumns;
    if (!effectiveAnalysisColumns.length) {
      setError("Выберите хотя бы одну колонку для анализа.");
      return;
    }
    const effectiveNonAnalysisColumns = groupByColumn ? [] : nonAnalysisColumns;
    const parsedMaxReviews = Math.floor(Number(maxReviewsInput));
    if (!Number.isFinite(parsedMaxReviews) || parsedMaxReviews < 1) {
      setError("Лимит отзывов должен быть целым числом от 1.");
      return;
    }
    // Do not hard-cap by inspect total_rows here: for very large files
    // inspect may return an approximate row count.
    const effectiveMaxReviews = parsedMaxReviews;
    const validationError = validateExpectedJsonTemplate(expectedJsonTemplate);
    if (validationError) {
      setError(validationError);
      return;
    }
    if (!providers.some((item) => item.id === provider)) {
      setError("Провайдер недоступен. Обновите список провайдеров.");
      return;
    }
    if (rememberToken) {
      sessionStorage.setItem(TOKEN_SESSION_KEY, apiKey);
    } else {
      sessionStorage.removeItem(TOKEN_SESSION_KEY);
    }

    setError("");
    setStatusLogs([
      `[${nowTsLabel()}] Отправляем задачу на сервер`,
      `[${nowTsLabel()}] Подготовка данных: ожидаем подтверждение запуска`,
    ]);
    setJob(null);
    setSelectedReportId(null);
    setSelectedReportAnalysis(null);
    stopJobTracking();
    setJobElapsedSec(0);
    setIsRunning(true);
    setJob({
      job_id: "pending",
      status: "queued",
      created_at: new Date().toISOString(),
      started_at: null,
      finished_at: null,
      total: 0,
      processed: 0,
      progress_percent: 0,
      eta_seconds: null,
      current_step: "Подготовка данных: отправка задачи",
      logs: [],
      result: {
        summary: null,
        preview_rows: [],
        results_file: null,
        raw_file: null,
      },
    });

    try {
      const newJobId = await api.startJob({
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
      setJobId(newJobId);
      setSelectedReportId(newJobId);
      setJob((prev) =>
        prev
          ? {
              ...prev,
              job_id: newJobId,
              current_step: "Подготовка данных: задача создана, ждём первые события",
            }
          : prev,
      );
      subscribeToEvents(newJobId);
      await loadReports();
    } catch (e) {
      setIsRunning(false);
      stopJobTracking();
      setError((e as Error).message);
    }
  }

  function subscribeToEvents(activeJobId: string) {
    stopJobTracking();
    // Сразу подтягиваем актуальный job, чтобы блок «3. Обработка» не висел на
    // «Ожидание запуска / Ожидание». SSE-обработчик ниже делает setJob((prev)=>prev)
    // при null — без инициализации статусы бы никогда не применились.
    (async () => {
      try {
        const initial = await api.getJob(activeJobId);
        setJob(initial);
        setJobId(activeJobId);
        if (Array.isArray(initial.logs) && initial.logs.length) {
          setStatusLogs(initial.logs.map((item) => String(item)));
        }
      } catch {
        // ignore: SSE заработает как только воркер стартанёт
      }
    })();
    const eventSource = new EventSource(api.jobEventsUrl(activeJobId), { withCredentials: true });
    eventSourceRef.current = eventSource;

    const hydrateCompletedReportAnalysis = async () => {
      try {
        const analysis = await api.getReportAnalysis(activeJobId);
        setSelectedReportId(activeJobId);
        setSelectedReportAnalysis(analysis);
      } catch {
        // ignore: fallback to report open action
      }
    };

    const stopPolling = () => {
      if (statusPollRef.current !== null) {
        window.clearInterval(statusPollRef.current);
        statusPollRef.current = null;
      }
    };
    const isTerminal = (status: string | undefined): boolean => status === "completed" || status === "failed" || status === "canceled";

    const startFallbackPolling = () => {
      if (statusPollRef.current !== null) return;
      statusPollRef.current = window.setInterval(async () => {
        try {
          const latest = await api.getJob(activeJobId);
          setJob(latest);
          if (Array.isArray(latest.logs) && latest.logs.length) {
            setStatusLogs(latest.logs.map((item) => String(item)));
          }
          if (isTerminal(latest.status)) {
            stopPolling();
            setIsRunning(false);
            await loadReports();
            if (latest.status === "completed") {
              await hydrateCompletedReportAnalysis();
            }
          }
        } catch {
          // keep fallback polling silently
        }
      }, JOB_STATUS_FALLBACK_POLL_MS);
    };

    eventSource.onmessage = async (message) => {
      const event = JSON.parse(message.data) as SseEvent;
      const payloadStatus = typeof event.payload.status === "string" ? event.payload.status : null;
      const payloadProcessed = typeof event.payload.processed === "number" ? event.payload.processed : null;
      const payloadTotal = typeof event.payload.total === "number" ? event.payload.total : null;
      const payloadEta = typeof event.payload.eta_seconds === "number" ? event.payload.eta_seconds : null;
      const payloadStep = typeof event.payload.current_step === "string" ? event.payload.current_step : null;
      const payloadMessage = typeof event.payload.message === "string" ? event.payload.message : null;
      const payloadLogs = event.payload.logs;

      if (payloadStatus || payloadProcessed !== null || payloadTotal !== null || payloadStep || payloadEta !== null) {
        setJob((prev) => {
          if (!prev) return prev;
          const processed = payloadProcessed ?? prev.processed;
          const total = payloadTotal ?? prev.total;
          const progressPercent = total > 0 ? Math.round((processed / total) * 10000) / 100 : prev.progress_percent;
          return {
            ...prev,
            status: (payloadStatus as typeof prev.status | null) ?? prev.status,
            processed,
            total,
            progress_percent: progressPercent,
            eta_seconds: payloadEta ?? prev.eta_seconds,
            current_step: payloadStep ?? prev.current_step,
          };
        });
        // Keep "Мои отчеты" visually fresh even with throttled full refreshes.
        void loadActiveReports();
      }
      if (payloadMessage) {
        setStatusLogs((prev) => [...prev.slice(-18), `[${nowTsLabel()}] ${payloadMessage}`]);
      }
      if (Array.isArray(payloadLogs)) {
        setStatusLogs(payloadLogs.map((item) => String(item)));
      }

      if (event.type === "done" || event.type === "error") {
        stopPolling();
        eventSource.close();
        eventSourceRef.current = null;
        const latest = await api.getJob(activeJobId);
        setJob(latest);
        setIsRunning(false);
        await loadReports(true);
        if (event.type === "done") {
          await hydrateCompletedReportAnalysis();
        }
        if (event.type === "error") {
          setError(String(event.payload.message || "Ошибка обработки"));
        }
      }

      if (payloadStatus && isTerminal(payloadStatus)) {
        stopPolling();
        eventSource.close();
        eventSourceRef.current = null;
        const latest = await api.getJob(activeJobId);
        setJob(latest);
        setIsRunning(false);
        await loadReports(true);
        if (payloadStatus === "completed") {
          await hydrateCompletedReportAnalysis();
        }
      }
    };

    eventSource.onerror = () => {
      eventSource.close();
      eventSourceRef.current = null;
      startFallbackPolling();
    };
  }

  async function cancelAnalysis() {
    const activeJobId = launchJobId;
    if (!activeJobId) return;
    try {
      await api.cancelJob(activeJobId);
      const latest = await api.getJob(activeJobId);
      setJobId(activeJobId);
      setJob(latest);
      setIsRunning(false);
      stopJobTracking();
      setActionToast({ message: "Задача отменена.", severity: "error" });
      await loadReports(true);
    } catch (e) {
      setError((e as Error).message);
    }
  }

  async function handleReportAction(jobIdValue: string, action: "pause" | "resume" | "cancel") {
    setError("");
    try {
      if (action === "pause") await api.pauseJob(jobIdValue);
      if (action === "resume") await api.resumeJob(jobIdValue);
      if (action === "cancel") await api.cancelJob(jobIdValue);
      if (action === "pause") setActionToast({ message: "Задача поставлена на паузу.", severity: "warning" });
      if (action === "resume") setActionToast({ message: "Задача запущена.", severity: "success" });
      if (action === "cancel") setActionToast({ message: "Задача отменена.", severity: "error" });

      const latest = await api.getJob(jobIdValue);
      setJobId(jobIdValue);
      setJob(latest);
      if (latest.status === "running" || latest.status === "queued") {
        setIsRunning(true);
        subscribeToEvents(jobIdValue);
      } else {
        setIsRunning(false);
        stopJobTracking();
      }
      await loadReports(true);
    } catch (e) {
      setError((e as Error).message);
    }
  }

  async function openReport(reportId: string) {
    setError("");
    setSelectedReportLoading(true);
    setSelectedReportLoadingId(reportId);
    try {
      const analysis = await api.getReportAnalysis(reportId);
      setSelectedReportId(reportId);
      setSelectedReportAnalysis(analysis);
      setStatusLogs([]);
      // Если отчёт всё ещё активен (running/queued/paused) — подписываемся на SSE,
      // чтобы блок «3. Обработка» показывал живой прогресс и статус. Для paused SSE
      // не шлёт событий, но subscribeToEvents сразу подтягивает job через
      // api.getJob() — этого достаточно, чтобы текст обновился.
      // Для терминальных (completed/failed/canceled) — чистим состояние как раньше.
      const reportRow = reports.find((r) => r.id === reportId);
      const activeStatus = reportRow?.status || "";
      if (["running", "queued", "paused"].includes(activeStatus) && reportRow?.job_id) {
        subscribeToEvents(reportRow.job_id);
      } else {
        setJob(null);
        setJobId(null);
        setIsRunning(false);
        stopJobTracking();
      }
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSelectedReportLoading(false);
      setSelectedReportLoadingId((prev) => (prev === reportId ? null : prev));
    }
  }

  async function deleteReport(reportId: string) {
    if (!window.confirm("Удалить отчет без возможности восстановления?")) return;
    setError("");
    try {
      await api.deleteReport(reportId);
      setActionToast({ message: "Отчет удален.", severity: "error" });
      if (jobId === reportId || job?.job_id === reportId) {
        stopJobTracking();
        setJobId(null);
        setJob(null);
        setStatusLogs([]);
        setIsRunning(false);
      }
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
      setActionToast({ message: "Перезапуск поставлен в очередь. Кэш включён.", severity: "success" });
      await loadReports(true);
      if (selectedReportId === reportId) {
        // Сбрасываем кэш analysis — иначе в виджете «4. Результаты» останется
        // старое превью и застывшая статистика до ручного перезахода на отчёт.
        // После null-а live-polling перезапустится и подтянет свежие данные.
        setSelectedReportAnalysis(null);
        subscribeToEvents(jobIdValue);
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
    // Для partial всегда активен только формат xlsx/raw — они соберутся на лету на бэке.
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
      filename: defaultName,
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
    const url = `${base}/${segment}/${format}?filename=${encodeURIComponent(safeFilename)}`;
    window.open(url, "_blank");
    setDownloadDialog(null);
  }


  async function savePreset() {
    const name = presetName.trim();
    if (!name) {
      setError("Введите имя пресета.");
      return;
    }
    const promptTemplate = promptEditorValue.trim();
    if (!promptTemplate) {
      setError("Промпт не должен быть пустым.");
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
      setError("Промпт не должен быть пустым.");
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
      const templateHintRaw = typeof raw.template_hint === "string" ? raw.template_hint : "";
      const importedName = typeof raw.name === "string" ? raw.name.trim() : "";

      if (!promptTemplate) {
        throw new Error("В файле пресета отсутствует корректный prompt_template.");
      }
      if (!expectedTemplate || typeof expectedTemplate !== "object" || Array.isArray(expectedTemplate)) {
        throw new Error("В файле пресета отсутствует корректный expected_json_template.");
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
      setActionToast({ message: "Пресет загружен.", severity: "success" });
    } catch (e) {
      setError(`Не удалось загрузить пресет: ${(e as Error).message}`);
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

  async function openAdminReport(reportId: string) {
    setError("");
    setSelectedReportLoading(true);
    setSelectedReportLoadingId(reportId);
    try {
      const analysis = await api.adminReportAnalysis(reportId);
      setSelectedReportId(reportId);
      setSelectedReportAnalysis(analysis);
      setJob(null);
      setJobId(null);
      setStatusLogs([]);
      setIsRunning(false);
      stopJobTracking();
    } catch (e) {
      setError((e as Error).message);
    } finally {
      setSelectedReportLoading(false);
      setSelectedReportLoadingId((prev) => (prev === reportId ? null : prev));
    }
  }

  async function handleAdminReportAction(reportId: string, action: "pause" | "resume" | "cancel" | "delete") {
    setError("");
    try {
      if (action === "pause") {
        await api.adminPauseReport(reportId);
        setActionToast({ message: "Задача пользователя поставлена на паузу.", severity: "warning" });
      }
      if (action === "resume") {
        await api.adminResumeReport(reportId);
        setActionToast({ message: "Задача пользователя запущена.", severity: "success" });
      }
      if (action === "cancel") {
        await api.adminCancelReport(reportId);
        setActionToast({ message: "Задача пользователя отменена.", severity: "error" });
      }
      if (action === "delete") {
        await api.adminDeleteReport(reportId);
        setActionToast({ message: "Отчет пользователя удален.", severity: "error" });
      }
      if (typeof adminSelectedUserId === "number") {
        await loadAdminReports(adminSelectedUserId);
      }
      await loadAdminOverview();
    } catch (e) {
      setError((e as Error).message);
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
    if (!window.confirm(`Удалить все отчеты (${reports.length}) без возможности восстановления?`)) return;
    setError("");
    setDeletingAllReports(true);
    try {
      const ids = reports.map((r) => r.id);
      const activeSet = new Set(ids);
      const outcomes = await Promise.allSettled(ids.map((id) => api.deleteReport(id)));
      const deleted = outcomes.filter((x) => x.status === "fulfilled").length;
      const failed = outcomes.length - deleted;

      if (jobId && activeSet.has(jobId)) {
        stopJobTracking();
        setJobId(null);
        setJob(null);
        setStatusLogs([]);
        setIsRunning(false);
      }

      if (failed === 0) {
        setActionToast({ message: `Удалены все отчеты: ${deleted}.`, severity: "error" });
      } else {
        setActionToast({ message: `Удалено: ${deleted}, не удалено: ${failed}.`, severity: "warning" });
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
    const reportId = job?.job_id || jobId;
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
    return reports.find((r) => r.id === selectedReportId) || null;
  }, [reports, selectedReportId]);

  const activeResult = useMemo(() => {
    const isAdmin = user?.role === "admin";
    const isNonTerminal = (status: string | undefined | null): boolean =>
      status === "queued" || status === "running" || status === "paused";
    if (job?.result?.summary) {
      const jobStatus = job.status as string | undefined;
      if (selectedReportId === job.job_id && selectedReportAnalysis?.summary) {
        return {
          title: isAdmin ? "Результаты выбранного отчета" : "4. Результаты",
          summary: selectedReportAnalysis.summary,
          previewRows: selectedReportAnalysis.preview_rows,
          inProgress: isNonTerminal(jobStatus),
          status: jobStatus ?? null,
          resultsFileLink: `${API_ROOT}/jobs/${job.job_id}/download/xlsx`,
          rawFileLink: job.result.raw_file
            ? `${API_ROOT}/jobs/${job.job_id}/download/raw`
            : null,
        };
      }
      return {
        title: isAdmin ? "Результаты выбранного отчета" : "4. Результаты",
        summary: job.result.summary,
        previewRows: job.result.preview_rows,
        inProgress: isNonTerminal(jobStatus),
        status: jobStatus ?? null,
        resultsFileLink: `${API_ROOT}/jobs/${job.job_id}/download/xlsx`,
        rawFileLink: job.result.raw_file
          ? `${API_ROOT}/jobs/${job.job_id}/download/raw`
          : null,
      };
    }
    if (selectedReportAnalysis?.summary) {
      const reportStatus = selectedReportAnalysis.status as string | undefined;
      const inProgress = isNonTerminal(reportStatus);
      // Пока отчёт в работе — подменяем summary живыми данными из /api/reports.
      // Preview-строки остаются замороженными (первые 10), чтобы не грузить БД
      // повторным чтением preview при активных 100+ пользователях.
      // Используем live summary_json из /api/reports только если бэкенд его прислал
      // с точными счётчиками success/failed. Синтезировать их из processed_rows НЕЛЬЗЯ:
      // processed считает и done, и error одинаково — иначе все провалившиеся по
      // context_exceeded строки показываются как успешные.
      const liveSummary: JobSummary | null =
        inProgress ? (selectedLaunchReport?.summary_json ?? null) : null;
      return {
        title: isAdmin ? "Результаты выбранного отчета" : "4. Результаты",
        summary: liveSummary ?? selectedReportAnalysis.summary,
        previewRows: selectedReportAnalysis.preview_rows,
        inProgress,
        status: reportStatus ?? null,
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
    }
    return null;
  }, [job, selectedReportAnalysis, selectedReportId, selectedLaunchReport, user?.role]);

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
      { name: "Успешно", value: done, fill: "#1f9d69" },
      { name: "С ошибками", value: failed, fill: "#d14334" },
    ];
  }, [activeResult]);

  const visibleReports = useMemo(() => {
    if (showAllReports) return reports;
    return reports.slice(0, 3);
  }, [reports, showAllReports]);


  // Live-превью: пока отчёт в работе (running/paused/queued), тянем analysis каждые 5 секунд —
  // но только пока в UI ещё нет 10 строк превью. После того как таблица превью заполнена,
  // поллинг останавливается: данные уже есть, повторные запросы в БД не нужны. Финальные
  // данные всё равно подтянутся через hydrateCompletedReportAnalysis в SSE-обработчике done.
  useEffect(() => {
    if (!selectedReportId) return;
    const liveStatus = (job?.status as string | undefined) || selectedLaunchReport?.status;
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
  }, [selectedReportId, job?.status, selectedLaunchReport?.status, user?.role, selectedReportAnalysis?.preview_rows?.length]);

  const launchJobId = useMemo(() => {
    if (jobId) return jobId;
    if (job && job.job_id !== "pending") return job.job_id;
    if (selectedLaunchReport && ["running", "queued", "paused"].includes(selectedLaunchReport.status)) {
      return selectedLaunchReport.job_id;
    }
    return null;
  }, [jobId, job, selectedLaunchReport]);

  const launchStatus = useMemo(() => {
    if (job?.status) return job.status;
    if (selectedLaunchReport && ["running", "queued", "paused"].includes(selectedLaunchReport.status)) {
      return selectedLaunchReport.status;
    }
    return null;
  }, [job, selectedLaunchReport]);

  // Статус выбранного отчёта вне зависимости от терминальности: нужен для
  // кнопок Перезапустить/Скачать в блоке «Запуск», которые должны отображаться
  // и на failed/canceled/completed (launchStatus такие статусы намеренно не отдаёт).
  const selectedLaunchStatus = useMemo<string | null>(() => {
    if (job?.status) return job.status as string;
    if (selectedLaunchReport?.status) return selectedLaunchReport.status;
    return null;
  }, [job, selectedLaunchReport]);

  if (!user) {
    return (
      <Box className="page">
      <Box className="hero" />
      <Box className="container">
        <Stack direction="row" justifyContent="space-between" alignItems="center">
          <Box className="brandBox">
            <Box className="brandLogoFrame">
              <img
                src={logoSrc}
                alt="Логотип"
                className="brandLogo"
                onError={handleLogoError}
              />
            </Box>
            <Box className="brandText">
              <Typography variant="h4" className="title">Личный кабинет</Typography>
            </Box>
          </Box>
          <Button variant="outlined" onClick={toggle} startIcon={mode === "dark" ? <LightModeIcon /> : <DarkModeIcon />}>
            {mode === "dark" ? "Светлая тема" : "Темная тема"}
          </Button>
        </Stack>
          <Typography className="subtitle">Войдите или зарегистрируйтесь, чтобы запускать фоновые задачи и хранить отчеты.</Typography>
          <Card className="card" sx={{ maxWidth: 560 }}>
            <CardContent>
              <Typography variant="h6">{authMode === "login" ? "Вход" : "Регистрация"}</Typography>
              <Stack spacing={2} sx={{ mt: 2 }}>
                <TextField label="Логин" value={authUsername} onChange={(e) => setAuthUsername(e.target.value)} />
                <TextField
                  label="Пароль"
                  type="password"
                  value={authPassword}
                  onChange={(e) => setAuthPassword(e.target.value)}
                  helperText={authMode === "register" ? "Минимум 8 символов, хотя бы 1 буква и 1 цифра" : undefined}
                />
                <Stack direction="row" spacing={1}>
                  <Button variant="contained" onClick={doAuth} disabled={authLoading || !authUsername || !authPassword}>
                    {authMode === "login" ? "Войти" : "Создать аккаунт"}
                  </Button>
                  <Button variant="outlined" onClick={() => setAuthMode(authMode === "login" ? "register" : "login")}>
                    {authMode === "login" ? "Нет аккаунта?" : "Уже есть аккаунт?"}
                  </Button>
                </Stack>
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

  return (
    <Box className="page">
      <Box className="hero" />
      <Box className="container">
        <Stack direction="row" justifyContent="space-between" alignItems="center">
          <Box className="brandBox">
            <Box className="brandLogoFrame">
              <img
                src={logoSrc}
                alt="Логотип"
                className="brandLogo"
                onError={handleLogoError}
              />
            </Box>
            <Box className="brandText">
              <Typography variant="h4" className="title">Анализатор отзывов</Typography>
            </Box>
          </Box>
          <Tooltip title="Меню">
            <IconButton
              onClick={(e) => setUserMenuAnchor(e.currentTarget)}
              aria-label="Меню"
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
          <Menu
            anchorEl={userMenuAnchor}
            open={Boolean(userMenuAnchor)}
            onClose={() => setUserMenuAnchor(null)}
            anchorOrigin={{ vertical: "bottom", horizontal: "right" }}
            transformOrigin={{ vertical: "top", horizontal: "right" }}
            PaperProps={{ sx: { minWidth: 260 } }}
          >
            <Box sx={{ px: 2, py: 1.25 }}>
              <Typography variant="subtitle2" sx={{ fontWeight: 600 }}>{user.username}</Typography>
              <Typography variant="caption" color="text.secondary">
                Роль: {user.role === "admin" ? "администратор" : "пользователь"}
              </Typography>
            </Box>
            <Divider />
            <Box sx={{ px: 2, py: 1 }}>
              <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 0.5 }}>
                Использовано токенов (всего)
              </Typography>
              <Typography variant="body2">
                prompt {usage?.prompt_tokens ?? 0} · completion {usage?.completion_tokens ?? 0} · total {usage?.total_tokens ?? 0}
              </Typography>
            </Box>
            <Divider />
            <MenuItem onClick={() => { setUserMenuAnchor(null); toggle(); }}>
              <ListItemIcon>
                {mode === "dark" ? <LightModeIcon fontSize="small" /> : <DarkModeIcon fontSize="small" />}
              </ListItemIcon>
              <ListItemText>{mode === "dark" ? "Светлая тема" : "Тёмная тема"}</ListItemText>
            </MenuItem>
            <MenuItem onClick={() => { setUserMenuAnchor(null); openReleaseNotes(); }}>
              <ListItemIcon><NewReleasesIcon fontSize="small" /></ListItemIcon>
              <ListItemText>Заметки о релизе</ListItemText>
            </MenuItem>
            <Divider />
            <MenuItem onClick={() => { setUserMenuAnchor(null); doLogout(); }}>
              <ListItemIcon><LogoutIcon fontSize="small" color="error" /></ListItemIcon>
              <ListItemText primaryTypographyProps={{ color: "error" }}>Выйти</ListItemText>
            </MenuItem>
            <Divider />
            <Box sx={{ px: 2, py: 0.75 }}>
              <Typography variant="caption" color="text.secondary">
                Анализатор отзывов · версия {APP_VERSION}
              </Typography>
            </Box>
          </Menu>
        </Stack>

        {user.role !== "admin" && (
        <Card className="card">
          <CardContent>
            <Stack direction="row" justifyContent="space-between" alignItems="center">
              <Typography variant="h6">Мои отчеты</Typography>
              <Stack direction="row" spacing={0.5} alignItems="center">
                <Tooltip title="Обновить">
                  <IconButton size="small" onClick={refreshReportsClick} aria-label="Обновить">
                    <RefreshIcon fontSize="small" />
                  </IconButton>
                </Tooltip>
                {(reports.length > 3 || reports.length > 0) && (
                  <Tooltip title="Управление списком">
                    <IconButton
                      size="small"
                      onClick={(ev) => setReportsMenuAnchor(ev.currentTarget)}
                      aria-label="Управление списком"
                    >
                      <MoreHorizIcon fontSize="small" />
                    </IconButton>
                  </Tooltip>
                )}
                <Menu
                  anchorEl={reportsMenuAnchor}
                  open={Boolean(reportsMenuAnchor)}
                  onClose={() => setReportsMenuAnchor(null)}
                  MenuListProps={{ dense: true }}
                >
                  {reports.length > 3 && (
                    <MenuItem onClick={() => { setReportsMenuAnchor(null); setShowAllReports((v) => !v); }}>
                      {showAllReports ? "Свернуть список" : `Показать ещё (${reports.length - 3})`}
                    </MenuItem>
                  )}
                  <MenuItem
                    onClick={() => { setReportsMenuAnchor(null); deleteAllReports(); }}
                    disabled={!reports.length || deletingAllReports}
                  >
                    <ListItemIcon><DeleteOutlineIcon fontSize="small" color="error" /></ListItemIcon>
                    <ListItemText primaryTypographyProps={{ color: "error" }}>Удалить все</ListItemText>
                  </MenuItem>
                </Menu>
              </Stack>
            </Stack>
            <TableContainer sx={{ mt: 1, overflowX: "hidden" }}>
            <Table size="small" sx={{ width: "100%", tableLayout: "fixed" }}>
              <TableHead>
                <TableRow>
                  <TableCell sx={{ width: "23%" }}>Отчёт</TableCell>
                  <TableCell sx={{ width: "16%" }}>Создан</TableCell>
                  <TableCell sx={{ width: "10%" }}>Статус</TableCell>
                  <TableCell sx={{ width: "28%" }}>Прогресс</TableCell>
                  <TableCell sx={{ width: "4%" }} align="center">Инфо</TableCell>
                  <TableCell sx={{ width: "19%" }} align="right">Действия</TableCell>
                </TableRow>
              </TableHead>
              <TableBody>
                {visibleReports.map((r) => {
                  const progress = computeReportProgress(r);
                  const { isGrouped, processed: progressProcessed, total: progressTotal, unit: progressUnit } = progress;
                  const progressPercent = Math.round(progress.percent * 10) / 10;
                  const etaLabel = formatEta(r.eta_seconds);
                  const step = visibleCurrentStep(r.current_step);
                  const reportTitle = r.source_filename?.trim() || "—";
                  const hasAnyDownload = Boolean(r.results_file) || Boolean(r.raw_file) || Boolean(r.uploaded_file_id);
                  const canPartialDownload = r.status === "running" || r.status === "paused";
                  const isTerminal = r.status === "completed" || r.status === "failed" || r.status === "canceled";
                  return (
                    <TableRow key={r.id}>
                      <TableCell sx={{ wordBreak: "break-word" }}>
                        <Typography variant="body2" fontWeight={500}>{reportTitle}</Typography>
                      </TableCell>
                      <TableCell sx={{ whiteSpace: "nowrap" }}>
                        <Typography variant="caption" color="text.secondary">{fmtDate(r.created_at)}</Typography>
                      </TableCell>
                      <TableCell>
                        <Chip
                          size="small"
                          label={statusLabel(r.status)}
                          color={statusColor(r.status)}
                          variant={r.status === "running" ? "filled" : "outlined"}
                        />
                      </TableCell>
                      <TableCell>
                        <LinearProgress
                          variant="determinate"
                          value={Math.max(0, Math.min(100, progressPercent))}
                          color={progressBarColor(r.status)}
                        />
                        <Typography variant="caption" color="text.secondary">
                          {progressProcessed}/{progressTotal} {progressUnit} ({progressPercent.toFixed(1)}%)
                          {etaLabel ? ` · ${etaLabel}` : ""}
                        </Typography>
                        {step && (
                          <Typography variant="caption" color="text.secondary" display="block">
                            {step}
                          </Typography>
                        )}
                        {r.status === "queued" && typeof r.queue_position === "number" && (
                          <Typography variant="caption" color="text.secondary" display="block">
                            {formatQueueHint(r.queue_position)}
                          </Typography>
                        )}
                      </TableCell>
                      <TableCell align="center">
                        <Tooltip
                          arrow
                          title={
                            <Box sx={{ fontSize: 12, lineHeight: 1.5 }}>
                              <div>Модель: <b>{r.provider}/{r.model}</b></div>
                              <div>Параллелизм: <b>{r.parallelism ?? "-"}</b></div>
                              <div>Лимит строк: <b>{r.max_reviews ?? "-"}</b></div>
                              <div>Температура: <b>{r.temperature ?? "-"}</b></div>
                              <div>Токены prompt: <b>{r.prompt_tokens ?? 0}</b></div>
                              <div>Токены completion: <b>{r.completion_tokens ?? 0}</b></div>
                              <div>Токены total: <b>{r.total_tokens ?? 0}</b></div>
                              {r.group_by_column && (<div>Группировка: <b>{r.group_by_column}</b></div>)}
                            </Box>
                          }
                        >
                          <InfoOutlinedIcon fontSize="small" color="action" sx={{ cursor: "help" }} />
                        </Tooltip>
                      </TableCell>
                      <TableCell align="right">
                        <Stack direction="row" spacing={0.25} justifyContent="flex-end" alignItems="center">
                          <Tooltip title="Открыть">
                            <span>
                              <IconButton
                                size="small"
                                color="info"
                                onClick={() => openReport(r.id)}
                                disabled={selectedReportLoadingId === r.id}
                                aria-label="Открыть"
                              >
                                <OpenInNewIcon fontSize="small" />
                              </IconButton>
                            </span>
                          </Tooltip>
                          {r.status === "running" && (
                            <Tooltip title="Пауза">
                              <IconButton size="small" color="warning" onClick={() => handleReportAction(r.job_id, "pause")} aria-label="Пауза">
                                <PauseIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {r.status === "paused" && (
                            <Tooltip title="Продолжить">
                              <IconButton size="small" color="success" onClick={() => handleReportAction(r.job_id, "resume")} aria-label="Продолжить">
                                <PlayArrowIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {(r.status === "failed" || r.status === "canceled") && (
                            <Tooltip title="Перезапустить (кэш включится принудительно)">
                              <span>
                                <IconButton
                                  size="small"
                                  color="secondary"
                                  onClick={() => retryReport(r.id, r.job_id)}
                                  disabled={retryingReportId === r.id}
                                  aria-label="Перезапустить"
                                >
                                  <RestartAltIcon fontSize="small" />
                                </IconButton>
                              </span>
                            </Tooltip>
                          )}
                          {(r.status === "running" || r.status === "queued" || r.status === "paused") && (
                            <Tooltip title="Отменить">
                              <IconButton size="small" color="error" onClick={() => handleReportAction(r.job_id, "cancel")} aria-label="Отменить">
                                <StopIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {canPartialDownload && (
                            <Tooltip title="Скачать промежуточный (соберётся на лету)">
                              <IconButton size="small" color="inherit" onClick={() => openDownloadDialog(r, "partial")} aria-label="Скачать промежуточный">
                                <DownloadIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {hasAnyDownload && isTerminal && (
                            <Tooltip title="Скачать…">
                              <IconButton size="small" color="success" onClick={() => openDownloadDialog(r, "completed")} aria-label="Скачать">
                                <DownloadIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                          {isTerminal && (
                            <Tooltip title="Удалить">
                              <IconButton size="small" color="error" onClick={() => deleteReport(r.id)} aria-label="Удалить">
                                <DeleteOutlineIcon fontSize="small" />
                              </IconButton>
                            </Tooltip>
                          )}
                        </Stack>
                      </TableCell>
                    </TableRow>
                  );
                })}
                {!visibleReports.length && (
                  <TableRow>
                    <TableCell colSpan={6}>Пока нет отчетов</TableCell>
                  </TableRow>
                )}
              </TableBody>
            </Table>
            </TableContainer>
          </CardContent>
        </Card>
        )}

        {user.role === "admin" && (
          <Card className="card">
            <CardContent>
              <Stack direction="row" justifyContent="space-between" alignItems="center">
                <Typography variant="h6">Админка</Typography>
                <Stack direction="row" spacing={1}>
                  <Button variant="outlined" onClick={loadAdminOverview} disabled={adminLoading}>
                    Обновить админ-данные
                  </Button>
                  <Button
                    variant={adminAutoRefresh ? "contained" : "outlined"}
                    color={adminAutoRefresh ? "success" : "inherit"}
                    onClick={() => setAdminAutoRefresh((v) => !v)}
                  >
                    {adminAutoRefresh ? "Автообновление: ВКЛ" : "Автообновление: ВЫКЛ"}
                  </Button>
                </Stack>
              </Stack>

              <Stack direction="row" spacing={1} sx={{ mt: 1.25 }} flexWrap="wrap" useFlexGap>
                <Chip label={`Очередь Redis: ${adminStats?.queue_depth ?? 0}`} />
                <Chip label={`queued: ${adminStats?.queued ?? 0}`} />
                <Chip label={`running: ${adminStats?.running ?? 0}`} color="success" />
                <Chip label={`paused: ${adminStats?.paused ?? 0}`} color="warning" />
                <Chip label={`failed: ${adminStats?.failed ?? 0}`} color="error" />
              </Stack>

              <Stack direction={{ xs: "column", md: "row" }} spacing={2} sx={{ mt: 2 }} alignItems={{ md: "flex-end" }}>
                <FormControl sx={{ minWidth: 320 }}>
                  <InputLabel id="admin-user-select-label">Пользователь</InputLabel>
                  <Select
                    labelId="admin-user-select-label"
                    value={adminSelectedUserId}
                    label="Пользователь"
                    onChange={(e) => {
                      const next = e.target.value;
                      setAdminSelectedUserId(next === "" ? "" : Number(next));
                    }}
                  >
                    {adminUsers.map((u) => (
                      <MenuItem key={u.id} value={u.id}>
                        {u.username} ({u.role}) - отчетов: {u.reports_count}
                      </MenuItem>
                    ))}
                  </Select>
                </FormControl>
                <Button
                  variant="outlined"
                  onClick={() => {
                    if (typeof adminSelectedUserId === "number") loadAdminReports(adminSelectedUserId);
                  }}
                  disabled={typeof adminSelectedUserId !== "number" || adminLoading}
                >
                  Обновить отчеты пользователя
                </Button>
              </Stack>

              <Typography variant="subtitle1" sx={{ mt: 2 }}>Отчеты выбранного пользователя</Typography>
              <TableContainer sx={{ mt: 1, overflowX: "hidden" }}>
                <Table size="small" sx={{ width: "100%", tableLayout: "fixed" }}>
                  <TableHead>
                    <TableRow>
                      <TableCell>Создан</TableCell>
                      <TableCell>Статус</TableCell>
                      <TableCell>Модель</TableCell>
                      <TableCell>Прогресс</TableCell>
                      <TableCell>Токены</TableCell>
                      <TableCell>Действия</TableCell>
                      <TableCell>Файлы</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {adminReports.map((r) => (
                      <TableRow key={`admin-${r.id}`}>
                        <TableCell>{fmtDate(r.created_at)}</TableCell>
                        <TableCell>{r.status}</TableCell>
                        <TableCell sx={{ wordBreak: "break-word" }}>{r.provider}/{r.model}</TableCell>
                        <TableCell>
                          <Typography variant="caption">
                            {r.processed_rows}/{r.total_rows} ({(r.progress_percent || 0).toFixed(1)}%)
                          </Typography>
                          <Typography variant="caption" color="text.secondary" display="block">
                            {r.current_step || "-"}
                          </Typography>
                        </TableCell>
                        <TableCell>{r.total_tokens ?? 0}</TableCell>
                        <TableCell>
                          <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                            <Button
                              size="small"
                              variant={selectedReportId === r.id ? "contained" : "outlined"}
                              onClick={() => openAdminReport(r.id)}
                              disabled={selectedReportLoadingId === r.id}
                            >
                              Открыть
                            </Button>
                            {r.status === "running" && (
                              <Button size="small" color="warning" onClick={() => handleAdminReportAction(r.id, "pause")}>
                                Пауза
                              </Button>
                            )}
                            {r.status === "paused" && (
                              <Button size="small" color="success" onClick={() => handleAdminReportAction(r.id, "resume")}>
                                Старт
                              </Button>
                            )}
                            {(r.status === "running" || r.status === "queued" || r.status === "paused") && (
                              <Button size="small" color="error" onClick={() => handleAdminReportAction(r.id, "cancel")}>
                                Отмена
                              </Button>
                            )}
                            {!["running", "queued", "paused"].includes(r.status) && (
                              <Button size="small" variant="outlined" color="error" onClick={() => handleAdminReportAction(r.id, "delete")}>
                                Удалить
                              </Button>
                            )}
                          </Stack>
                        </TableCell>
                        <TableCell>
                          <Stack direction="row" spacing={0.5} flexWrap="wrap" useFlexGap>
                            {r.results_file && (
                              <Button size="small" sx={{ minWidth: 0, px: 1 }} href={api.adminReportDownloadUrl(r.id, "xlsx")} target="_blank">
                                XLSX
                              </Button>
                            )}
                            {r.raw_file && (
                              <Button size="small" sx={{ minWidth: 0, px: 1 }} href={api.adminReportDownloadUrl(r.id, "raw")} target="_blank">
                                JSON
                              </Button>
                            )}
                            {r.uploaded_file_id && (
                              <Button size="small" sx={{ minWidth: 0, px: 1 }} href={api.adminReportDownloadUrl(r.id, "source")} target="_blank">
                                RAW
                              </Button>
                            )}
                          </Stack>
                        </TableCell>
                      </TableRow>
                    ))}
                    {!adminReports.length && (
                      <TableRow>
                        <TableCell colSpan={7}>Нет отчетов по выбранному пользователю</TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </TableContainer>

              <Typography variant="subtitle1" sx={{ mt: 2 }}>Последние ошибки задач</Typography>
              <TableContainer sx={{ mt: 1, overflowX: "hidden" }}>
                <Table size="small" sx={{ width: "100%", tableLayout: "fixed" }}>
                  <TableHead>
                    <TableRow>
                      <TableCell>Время</TableCell>
                      <TableCell>Пользователь</TableCell>
                      <TableCell>Report</TableCell>
                      <TableCell>Ошибка</TableCell>
                    </TableRow>
                  </TableHead>
                  <TableBody>
                    {(adminStats?.recent_failures || []).map((f) => (
                      <TableRow key={`fail-${f.report_id}-${f.updated_at || ""}`}>
                        <TableCell>{fmtDate(f.updated_at)}</TableCell>
                        <TableCell>{f.username}</TableCell>
                        <TableCell>{f.report_id}</TableCell>
                        <TableCell sx={{ wordBreak: "break-word" }}>{f.error_text || "-"}</TableCell>
                      </TableRow>
                    ))}
                    {!adminStats?.recent_failures?.length && (
                      <TableRow>
                        <TableCell colSpan={4}>Ошибок нет</TableCell>
                      </TableRow>
                    )}
                  </TableBody>
                </Table>
              </TableContainer>

              <Typography variant="subtitle1" sx={{ mt: 2 }}>Логи</Typography>
              <Stack direction={{ xs: "column", md: "row" }} spacing={1} sx={{ mt: 1 }} alignItems={{ md: "center" }}>
                <FormControl size="small" sx={{ minWidth: 140 }}>
                  <InputLabel id="admin-log-service">Сервис</InputLabel>
                  <Select
                    labelId="admin-log-service"
                    value={adminLogService}
                    label="Сервис"
                    onChange={(e) => setAdminLogService(String(e.target.value) as "all" | "backend" | "worker")}
                  >
                    <MenuItem value="all">Все</MenuItem>
                    <MenuItem value="backend">backend</MenuItem>
                    <MenuItem value="worker">worker</MenuItem>
                  </Select>
                </FormControl>
                <FormControl size="small" sx={{ minWidth: 140 }}>
                  <InputLabel id="admin-log-level">Уровень</InputLabel>
                  <Select
                    labelId="admin-log-level"
                    value={adminLogLevel}
                    label="Уровень"
                    onChange={(e) => setAdminLogLevel(String(e.target.value) as "" | "INFO" | "WARNING" | "ERROR")}
                  >
                    <MenuItem value="">Все</MenuItem>
                    <MenuItem value="INFO">INFO</MenuItem>
                    <MenuItem value="WARNING">WARNING</MenuItem>
                    <MenuItem value="ERROR">ERROR</MenuItem>
                  </Select>
                </FormControl>
                <TextField
                  size="small"
                  label="Поиск"
                  value={adminLogQuery}
                  onChange={(e) => setAdminLogQuery(e.target.value)}
                  sx={{ minWidth: 220 }}
                />
                <TextField
                  size="small"
                  type="number"
                  label="Строк"
                  value={adminLogLinesLimit}
                  onChange={(e) => {
                    const next = Number(e.target.value || 200);
                    setAdminLogLinesLimit(Math.max(10, Math.min(1000, Number.isFinite(next) ? next : 200)));
                  }}
                  sx={{ width: 110 }}
                />
                <Button variant="outlined" onClick={loadAdminLogs} disabled={adminLogsLoading}>
                  Обновить логи
                </Button>
                <Button
                  variant={adminLogsAutoRefresh ? "contained" : "outlined"}
                  color={adminLogsAutoRefresh ? "success" : "inherit"}
                  onClick={() => setAdminLogsAutoRefresh((v) => !v)}
                >
                  {adminLogsAutoRefresh ? "Автообновление логов: ВКЛ" : "Автообновление логов: ВЫКЛ"}
                </Button>
              </Stack>
              <Box className="logs" sx={{ mt: 1, maxHeight: 280 }}>
                {sortedAdminLogs.map((line, idx) => (
                  <Typography
                    key={`${line.ts || ""}-${idx}`}
                    variant="body2"
                    className="logline"
                    sx={{ fontFamily: "ui-monospace, SFMono-Regular, Menlo, Monaco, Consolas, monospace" }}
                  >
                    [{line.ts ? fmtDate(line.ts) : "-"}] [{line.service || "-"}] [{line.level || "-"}]
                    {line.request_id ? ` [${line.request_id}]` : ""}
                    {line.user_id && line.user_id !== "-" ? ` [user_id=${line.user_id}]` : ""}
                    {line.username && line.username !== "-" ? ` [username=${line.username}]` : ""} {line.message}
                  </Typography>
                ))}
                {!adminLogs.length && (
                  <Typography variant="body2" color="text.secondary">
                    Логов пока нет
                  </Typography>
                )}
              </Box>
            </CardContent>
          </Card>
        )}

        {user.role !== "admin" && (
        <Card className="card">
          <CardContent>
            <Typography variant="h6">1. Данные и модель</Typography>
            <input
              ref={fileInputRef}
              hidden
              type="file"
              accept=".xlsx"
              onChange={(e) => {
                handleFileUpload(e.target.files?.[0]);
                // Сбрасываем value, чтобы onChange сработал даже если пользователь
                // выбрал тот же файл повторно (например, после ошибки).
                if (fileInputRef.current) fileInputRef.current.value = "";
              }}
            />
            <Stack direction={{ xs: "column", md: "row" }} spacing={1.5} alignItems="stretch">
            <Box
              className={`dropzone ${dragActive ? "active" : ""}`}
              onDragOver={(e) => {
                e.preventDefault();
                setDragActive(true);
              }}
              onDragLeave={() => setDragActive(false)}
              onDrop={onDrop}
              sx={{ textAlign: "center", py: 3, flex: 1 }}
            >
              {fileInfo ? (
                // Файл загружен — показываем имя и формат
                <Stack direction="row" spacing={1.5} alignItems="center" justifyContent="center">
                  <DescriptionIcon color="success" sx={{ fontSize: 48 }} />
                  <Stack direction="column" alignItems="flex-start">
                    <Typography variant="body1" fontWeight={500}>
                      {fileInfo.filename}
                    </Typography>
                    <Typography variant="caption" color="text.secondary">
                      Формат XLSX · Нажмите «
                      <Link
                        component="button"
                        type="button"
                        onClick={() => fileInputRef.current?.click()}
                        sx={{ cursor: "pointer" }}
                      >
                        загрузить другой файл
                      </Link>
                      », если нужно заменить
                    </Typography>
                  </Stack>
                </Stack>
              ) : (
                <Stack direction="column" spacing={1} alignItems="center">
                  <CloudUploadIcon color="action" sx={{ fontSize: 40 }} />
                  <Typography>
                    Переместите файл сюда или{" "}
                    <Link
                      component="button"
                      type="button"
                      onClick={() => fileInputRef.current?.click()}
                      sx={{ cursor: "pointer", fontWeight: 500 }}
                    >
                      загрузите вручную
                    </Link>
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Поддерживаемый формат: XLSX
                  </Typography>
                </Stack>
              )}
            </Box>
            {!fileInfo && (
              <Button
                variant="outlined"
                onClick={openExamples}
                startIcon={<FolderOpenIcon />}
                sx={{ minWidth: { md: 180 }, whiteSpace: "nowrap" }}
              >
                Примеры файлов
              </Button>
            )}
            </Stack>

            {(fileInspectStatus === "queued" || fileInspectStatus === "parsing") && (
              <Box sx={{ mt: 2 }}>
                <LinearProgress />
                <Typography variant="caption" color="text.secondary" sx={{ mt: 0.75, display: "block" }}>
                  {fileInspectMessage || "Подготовка файла..."}
                </Typography>
                {typeof fileInfo?.queue_position === "number" && (
                  <Typography variant="caption" color="text.secondary" sx={{ display: "block" }}>
                    {formatQueueHint(fileInfo.queue_position)}
                  </Typography>
                )}
              </Box>
            )}

            {fileInfo && (
              <Stack direction={{ xs: "column", md: "row" }} spacing={2} sx={{ mt: 2 }} alignItems="stretch">
                <FormControl fullWidth>
                  <InputLabel id="sheet-label" shrink>
                    <FieldLabelContent label="Лист" hint="Страница Excel-файла, строки которой пойдут в анализ." />
                  </InputLabel>
                  <Select
                    labelId="sheet-label"
                    label="Лист ⓘ"
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
                      label="Колонки для анализа"
                      hint="Эти поля отправятся модели на анализ. В итоговый отчёт они не копируются — если нужны в xlsx, добавьте их в «Колонки из исходника в итоговый отчёт»."
                    />
                  </InputLabel>
                  <Select
                    labelId="analysis-cols-label"
                    label="Колонки для анализа ⓘ"
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

                <FormControl fullWidth disabled={Boolean(groupByColumn)}>
                  <InputLabel id="non-analysis-cols-label" shrink>
                    <FieldLabelContent
                      label="Колонки в итоговый отчёт"
                      hint={groupByColumn
                        ? "При группировке недоступно — значения внутри группы могут различаться."
                        : "Копируются в итоговый xlsx без обработки моделью. Можно выбирать те же колонки, что и для анализа — в итоге появятся один раз."}
                    />
                  </InputLabel>
                  <Select
                    labelId="non-analysis-cols-label"
                    label="Колонки в итоговый отчёт ⓘ"
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
                      label="Группировка (опц.)"
                      hint="Строки с одинаковым значением этой колонки обрабатываются как одна группа — один запрос в модель на всю группу, результат применяется ко всем строкам."
                    />
                  </InputLabel>
                  <Select
                    labelId="group-by-label"
                    label="Группировка (опц.) ⓘ"
                    notched
                    displayEmpty
                    value={groupByColumn}
                    onChange={(e) => setGroupByColumn(String(e.target.value))}
                  >
                    <MenuItem value="">Без группировки</MenuItem>
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
                  <Typography variant="subtitle1" fontWeight={500}>
                    Продвинутые настройки LLM
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ ml: 2, alignSelf: "center" }}>
                    Провайдер, модель, токен, лимиты, кэш
                  </Typography>
                </AccordionSummary>
                <AccordionDetails>
                  <Stack direction={{ xs: "column", md: "row" }} spacing={2} alignItems="stretch">
                    <FormControl fullWidth>
                      <InputLabel id="provider-label" shrink>
                        <FieldLabelContent
                          label="Провайдер"
                          hint="Выбери, куда будет уходить запрос: OpenAI-совместимая платформа или локальный LLM (Ollama)."
                        />
                      </InputLabel>
                      <Select
                        labelId="provider-label"
                        label="Провайдер ⓘ"
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
                          label="Модель"
                          hint="Конкретная модель выбранного провайдера. Мощнее — точнее, но дороже по токенам и медленнее."
                        />
                      </InputLabel>
                      <Select
                        labelId="model-label"
                        label="Модель ⓘ"
                        notched
                        displayEmpty
                        value={model}
                        onChange={(e) => setModel(String(e.target.value))}
                      >
                        {models.map((item) => (
                          <MenuItem key={item} value={item}>{item}</MenuItem>
                        ))}
                      </Select>
                    </FormControl>

                    {provider === "openai" && (
                      <Stack direction="column" spacing={0.5} sx={{ width: "100%" }}>
                        <Stack direction="row" spacing={1} alignItems="stretch">
                          <TextField
                            fullWidth
                            type="password"
                            label={(
                              <FieldLabelContent
                                label="API-токен"
                                hint={apiKey.trim() ? "Используется твой токен из этого поля." : "Поле пустое — сервер возьмёт ключ из переменной OPENAI_API_KEY."}
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
                            {tokenVerifyLoading ? "Проверяем…" : "Проверить"}
                          </Button>
                        </Stack>
                        {tokenVerifyResult?.ok && (
                          <Typography variant="caption" color="success.main" sx={{ wordBreak: "break-word" }}>
                            {`OK (моделей: ${tokenVerifyResult.models.length})`}
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
                          label="Лимит отзывов"
                          hint="Максимум строк, которые уйдут в модель в этом запуске. Остальные строки файла будут проигнорированы."
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
                          label="Параллелизм"
                          hint={`Сколько строк обрабатываются одновременно. Выше — быстрее, но выше нагрузка и риск rate-limit провайдера (макс ${parallelismMax}).`}
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
                          label="Температура"
                          hint="0 — детерминированный ответ, 2 — максимально творческий. Для классификации и структурированного анализа оставляй 0."
                        />
                      )}
                      InputLabelProps={{ shrink: true }}
                      value={temperature}
                      onChange={(e) => setTemperature(Math.max(0, Math.min(2, Number(e.target.value) || 0)))}
                      inputProps={{ min: 0, max: 2, step: 0.1 }}
                    />
                  </Stack>

                  {selectedSheetRows > 0 && (
                    <Typography variant="body2" color="text.secondary" sx={{ mt: 1.5 }}>
                      В выбранном листе строк с данными: {selectedSheetRows}
                      {groupByColumn && selectedGroupCount !== null && selectedGroupCount !== undefined && (
                        <>
                          {" "}· будет обработано <b>{selectedGroupCount.toLocaleString("ru-RU")}</b> {pluralizeGroups(selectedGroupCount)} (колонка «{groupByColumn}»)
                        </>
                      )}
                      {groupByColumn && selectedGroupCount === null && (
                        <>
                          {" "}· групп слишком много (более 100 000 уникальных значений) — проверь колонку группировки, возможно она не годится для агрегации
                        </>
                      )}
                    </Typography>
                  )}

                  <Stack direction="row" spacing={1} sx={{ mt: 2 }} flexWrap="wrap" useFlexGap>
                    {provider === "openai" && (
                      <Tooltip arrow title="Токен запомнится в этой вкладке браузера до её закрытия — после F5 вводить заново не нужно. При закрытии вкладки или окна токен пропадает.">
                        <Chip
                          label="Помнить токен на эту вкладку"
                          color={rememberToken ? "info" : "default"}
                          variant={rememberToken ? "filled" : "outlined"}
                          onClick={() => setRememberToken((v) => !v)}
                        />
                      </Tooltip>
                    )}
                    {provider === "openai" && (
                      <Tooltip arrow title="Токен шифруется и сохраняется на сервере, чтобы после рестарта сервиса (backend/worker) задачу можно было продолжить без повторного ввода. Нужно только если ставишь свою задачу на длительную обработку.">
                        <Chip
                          label="Помнить токен после перезапуска сервиса"
                          color={saveApiKeyForResume ? "info" : "default"}
                          variant={saveApiKeyForResume ? "filled" : "outlined"}
                          onClick={() => setSaveApiKeyForResume((v) => !v)}
                        />
                      </Tooltip>
                    )}
                    <Tooltip arrow title="Если модель уже отвечала на такой же отзыв с тем же промптом — берём сохранённый ответ и не тратим токены. Выключай только для тестов с одинаковыми входными данными и ожиданием разных ответов.">
                      <Chip
                        label="Использовать кэш ответов модели"
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

        {user.role !== "admin" && (
        <Card className="card">
          <CardContent>
            <Stack direction="row" alignItems="center" spacing={1}>
              <Typography variant="h6">2. Шаблон</Typography>
              <Chip
                icon={<InfoOutlinedIcon />}
                label={templateInfoExpanded ? "Скрыть инструкцию" : "Как это работает"}
                size="small"
                color="primary"
                variant={templateInfoExpanded ? "filled" : "outlined"}
                onClick={() => setTemplateInfoExpanded((v) => !v)}
                sx={{ fontWeight: 600, cursor: "pointer" }}
              />
            </Stack>
            <Collapse in={templateInfoExpanded}>
              <Alert severity="info" sx={{ mt: 1 }}>
                <Typography variant="body2" sx={{ fontWeight: 700, mb: 0.5 }}>Что такое «Шаблон» — в двух словах</Typography>
                <Typography variant="body2">
                  Это то, что вы объясняете модели: <strong>какую задачу решить</strong> (промпт) и <strong>в каком виде вернуть ответ</strong> (поля ответа).
                  Один раз настроили шаблон — дальше сервис прогонит его по всему загруженному файлу, строка за строкой.
                </Typography>

                <Typography variant="body2" sx={{ mt: 1.5, fontWeight: 700 }}>Быстрый старт на тестовом файле</Typography>
                <Typography variant="body2">
                  Если хочется сразу посмотреть как всё работает — в блоке «1. Данные и модель» справа от зоны загрузки есть кнопка <strong>«Примеры файлов»</strong>.
                  В ней лежат готовые xlsx с отзывами клиентов (1 000 и 100 000 строк). Выбираете файл, подбираете под него один из двух готовых пресетов и жмёте запуск.
                  Ниже — какие колонки куда поставить под каждый сценарий.
                </Typography>

                <Typography variant="body2" sx={{ mt: 1, fontWeight: 600 }}>Сценарий А. Построчная проверка каждого отзыва</Typography>
                <Typography variant="body2">
                  Пресет: <strong>«Пример: Проверка отзывов на мошенничество сотрудников»</strong>. Модель оценит каждый отзыв по отдельности.
                </Typography>
                <Typography variant="body2">— <em>Лист</em>: первый (единственный в файле).</Typography>
                <Typography variant="body2">— <em>Колонки для анализа</em>: <code>отзыв</code> — единственное поле, которое реально уходит в модель.</Typography>
                <Typography variant="body2">— <em>Колонки из исходника в итоговый отчёт</em>: <code>id</code>, <code>магазин</code>, <code>смена</code>, <code>оценка</code>, <code>дата</code> — попадут в итоговый xlsx без отправки в модель, чтобы потом можно было сопоставить результат с исходной записью.</Typography>
                <Typography variant="body2">— <em>Группировка</em>: оставить пустой.</Typography>
                <Typography variant="body2">— В блоке «2. Шаблон» в списке «Пресет» выбрать тот же построчный пресет — промпт и поля ответа подставятся сами.</Typography>
                <Typography variant="body2">— В блоке «3. Обработка» — «Запустить новый анализ». Каждая строка получит категорию, тип нарушения, уверенность и описание.</Typography>

                <Typography variant="body2" sx={{ mt: 1, fontWeight: 600 }}>Сценарий Б. Сводка по сменам (групповой анализ)</Typography>
                <Typography variant="body2">
                  Пресет: <strong>«Пример: Анализ мошенничества по магазинам (группировка)»</strong>. Модель получит отзывы одной смены целиком и вернёт один агрегат на группу.
                </Typography>
                <Typography variant="body2">— <em>Лист</em>: первый.</Typography>
                <Typography variant="body2">— <em>Колонки для анализа</em>: <code>отзыв</code>.</Typography>
                <Typography variant="body2">— <em>Группировка</em>: <code>смена</code> — это главное отличие от сценария А. Модель увидит все отзывы смены разом и оценит её целиком.</Typography>
                <Typography variant="body2">— <em>Колонки из исходника в итоговый отчёт</em>: в групповом режиме это поле блокируется — в итоговый xlsx попадает только колонка группировки и ответ модели (одна строка на группу).</Typography>
                <Typography variant="body2">— В блоке «2. Шаблон» — пресет «Анализ мошенничества по магазинам (группировка)».</Typography>
                <Typography variant="body2">— В результате каждая смена получит: число подтверждённых нарушений, долю в процентах, встреченные схемы, уровень риска (низкий / средний / высокий) и краткое описание.</Typography>

                <Typography variant="body2" sx={{ mt: 0.75, fontStyle: "italic", color: "text.secondary" }}>
                  Совет: для обоих сценариев берите файл на 1 000 строк — прогон занимает минуты, можно быстро оценить качество. Файл на 100 000 строк — для нагрузочного теста после того, как шаблон устраивает.
                </Typography>

                <Typography variant="body2" sx={{ mt: 1.5, fontWeight: 700 }}>Как сделать под свою задачу</Typography>
                <Typography variant="body2" sx={{ mt: 0.5, fontWeight: 600 }}>Шаг 1. Напишите промпт</Typography>
                <Typography variant="body2">
                  Промпт — это текст инструкции для модели. Пишите его как задачу коллеге: короткими и понятными фразами.
                </Typography>
                <Typography variant="body2">— Объясните, что модель должна сделать с одной записью из файла (отзывом, заявкой, сообщением).</Typography>
                <Typography variant="body2">— Опишите критерии. Например: «позитивный» — клиент доволен, «негативный» — есть явная жалоба. Не оставляйте на угадывание.</Typography>
                <Typography variant="body2">— Если тема сложная — дайте 1–2 коротких примера прямо в промпте.</Typography>
                <Typography variant="body2">
                  — В промпте <strong>не нужно</strong> писать «вот отзыв: …» или подставлять содержимое ячеек вручную.
                  Сервис сам на каждой итерации берёт очередную строку из вашего файла и передаёт её модели вместе с вашим промптом.
                </Typography>

                <Typography variant="body2" sx={{ mt: 0.75, fontWeight: 600 }}>Шаг 2. Настройте поля ответа</Typography>
                <Typography variant="body2">
                  Это «анкета», которую модель обязана заполнить по каждой строке. Называйте поля так, как вам удобно — хоть по-русски.
                </Typography>
                <Typography variant="body2">— Для каждого поля укажите тип: текст, число, дата, список-выбор, да/нет, массив.</Typography>
                <Typography variant="body2">— Если ответ должен быть из фиксированного набора (например, «позитивный / нейтральный / негативный») — используйте тип «список-выбор». Тогда модель не придумает своих категорий.</Typography>
                <Typography variant="body2">— Для чисел задайте диапазон (например, 1..5), для длинных текстов — ограничение длины, иначе модель напишет целое эссе.</Typography>
                <Typography variant="body2">— Добавляйте любые поля под свою задачу. Сервис сам проверит ответ модели и, если формат не сошёлся, отправит строку на повторную попытку с объяснением, что исправить.</Typography>

                <Typography variant="body2" sx={{ mt: 0.75, fontWeight: 600 }}>Шаг 3. Запустите и наблюдайте</Typography>
                <Typography variant="body2">
                  — Провайдер и модель настраиваются в блоке «1. Данные и модель» (раскройте «Продвинутые настройки LLM»). По умолчанию уже подставлен рабочий набор —
                  если не хотите менять, переходите дальше.
                </Typography>
                <Typography variant="body2">— В блоке «3. Обработка» нажмите «Запустить новый анализ».</Typography>
                <Typography variant="body2">
                  — В блоке «4. Результаты» прямо во время обработки появляются первые готовые строки.
                  На них удобно сразу увидеть, даёт ли промпт нужный результат, не дожидаясь конца всего отчёта.
                </Typography>
                <Typography variant="body2">
                  — Если качество не то — нажмите «Остановить», поправьте промпт или поля, затем снова «Запустить новый анализ».
                  Цена ошибки в начале — секунды, в конце — полчаса.
                </Typography>
                <Typography variant="body2">— Готовый отчёт скачивается в xlsx в блоке «4. Результаты». Промежуточные выгрузки доступны и в процессе.</Typography>

                <Typography variant="body2" sx={{ mt: 1.5, fontWeight: 700 }}>Про модели (практика)</Typography>
                <Typography variant="body2">
                  Для простых задач (тональность, классификация) подойдёт любая модель. Если в промпте сложные правила с «если…и…иначе…» или числовыми порогами —
                  берите крупные instruct-модели (например, Qwen3-235B, Claude Sonnet/Opus), слабые модели на таких правилах промахиваются.
                </Typography>

                <Typography variant="body2" sx={{ mt: 1, fontWeight: 700 }}>Пресеты и свои шаблоны</Typography>
                <Typography variant="body2">
                  В выпадающем списке «Пресет» сейчас два готовых примера — построчный и групповой анализ мошенничества. Их удобно взять как стартовую точку,
                  даже если ваша задача другая: посмотрите как устроен промпт и поля ответа, замените под себя. Свою версию можно сохранить как личный пресет,
                  чтобы возвращаться к ней в следующих отчётах. Библиотеку готовых пресетов будем расширять.
                </Typography>

              </Alert>
            </Collapse>
            <Stack direction={{ xs: "column", md: "row" }} spacing={1} alignItems={{ xs: "stretch", md: "center" }} sx={{ mt: 1 }}>
              <FormControl sx={{ minWidth: 240, flex: 1 }}>
                <InputLabel>Пресет</InputLabel>
                <Select
                  value={presetPickerValue}
                  label="Пресет"
                  onChange={(e) => { void applyPresetSelection(String(e.target.value)); }}
                >
                  {presets.map((preset) => (
                    <MenuItem key={preset.id} value={`user:${preset.id}`}>{`Личный: ${preset.name}`}</MenuItem>
                  ))}
                  {READY_PRESETS.map((preset) => (
                    <MenuItem key={preset.id} value={`ready:${preset.id}`}>{preset.label}</MenuItem>
                  ))}
                </Select>
              </FormControl>
              <TextField
                label="Имя пресета"
                value={presetName}
                onChange={(e) => setPresetName(e.target.value)}
                sx={{ minWidth: 200, flex: 1 }}
              />
              <Stack direction="row" spacing={0.5} alignItems="center">
                <Tooltip arrow title="Сохранить пресет">
                  <IconButton color="success" onClick={savePreset} aria-label="Сохранить пресет">
                    <SaveIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip arrow title="Загрузить пресет из файла">
                  <IconButton color="info" onClick={() => presetUploadInputRef.current?.click()} aria-label="Загрузить пресет из файла">
                    <CloudUploadIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip arrow title="Выгрузить пресет в файл">
                  <IconButton color="success" onClick={downloadPreset} aria-label="Выгрузить пресет в файл">
                    <DownloadIcon />
                  </IconButton>
                </Tooltip>
                <Tooltip arrow title={presetPickerValue.startsWith("user:") ? "Удалить пресет" : "Удалить можно только личный пресет"}>
                  <span>
                    <IconButton
                      color="error"
                      onClick={deleteSelectedPreset}
                      disabled={!presetPickerValue.startsWith("user:")}
                      aria-label="Удалить пресет"
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
              label="Промпт"
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
                  <Typography variant="subtitle1">Настройка полей ответа</Typography>
                  <Typography variant="body2" color="text.secondary">
                    Здесь настраиваются поля, которые модель должна вернуть по итогам анализа.
                  </Typography>
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
                  label="Расширенные настройки полей (длина, диапазон, формат даты, мин/макс элементов)"
                />
                <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 1.5 }}>
                  Названия полей можно задавать на русском или латинице — модель поймёт любой вариант.
                </Typography>
                <Stack spacing={1.25}>
                  {schemaFields.map((field, index) => (
                    <Card key={field.id} variant="outlined">
                      <CardContent>
                        <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} alignItems={{ md: "center" }}>
                          <TextField
                            label="Название поля"
                            placeholder="Например: тональность, категория, краткое_описание"
                            value={field.name}
                            onChange={(e) => updateSchemaField(field.id, { name: e.target.value })}
                            fullWidth
                          />
                          <FormControl sx={{ minWidth: 180 }}>
                            <InputLabel>Тип</InputLabel>
                            <Select
                              value={field.type}
                              label="Тип"
                              onChange={(e) => updateSchemaField(field.id, { type: e.target.value as BuilderFieldType })}
                            >
                              <MenuItem value="text">Текст</MenuItem>
                              <MenuItem value="number">Число</MenuItem>
                              <MenuItem value="datetime">Дата и время</MenuItem>
                              <MenuItem value="list">Список</MenuItem>
                            </Select>
                          </FormControl>
                          <Stack direction="row" spacing={1}>
                            <Button variant="outlined" onClick={() => moveSchemaField(field.id, -1)} disabled={index === 0}>
                              Вверх
                            </Button>
                            <Button
                              variant="outlined"
                              onClick={() => moveSchemaField(field.id, 1)}
                              disabled={index === schemaFields.length - 1}
                            >
                              Вниз
                            </Button>
                            <Button variant="outlined" color="error" onClick={() => removeSchemaField(field.id)}>
                              Удалить
                            </Button>
                          </Stack>
                        </Stack>

                        {field.type === "text" && showAdvancedSchemaAttrs && (
                          <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} sx={{ mt: 1.25 }}>
                            <TextField
                              label="Мин. длина"
                              value={field.textMinLength}
                              onChange={(e) => updateSchemaField(field.id, { textMinLength: e.target.value })}
                              sx={{ minWidth: 160 }}
                            />
                            <TextField
                              label="Макс. длина"
                              value={field.textMaxLength}
                              onChange={(e) => updateSchemaField(field.id, { textMaxLength: e.target.value })}
                              sx={{ minWidth: 160 }}
                            />
                          </Stack>
                        )}

                        {field.type === "number" && showAdvancedSchemaAttrs && (
                          <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} sx={{ mt: 1.25 }}>
                            <TextField
                              label="Минимум"
                              value={field.numberMin}
                              onChange={(e) => updateSchemaField(field.id, { numberMin: e.target.value })}
                              sx={{ minWidth: 180 }}
                            />
                            <TextField
                              label="Максимум"
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
                              label="Только целое"
                            />
                          </Stack>
                        )}

                        {field.type === "datetime" && showAdvancedSchemaAttrs && (
                          <Stack direction={{ xs: "column", md: "row" }} spacing={1.25} sx={{ mt: 1.25 }}>
                            <FormControl sx={{ minWidth: 220 }}>
                              <InputLabel>Формат</InputLabel>
                              <Select
                                value={field.datetimeMode}
                                label="Формат"
                                onChange={(e) => updateSchemaField(field.id, { datetimeMode: e.target.value as BuilderDateMode })}
                              >
                                <MenuItem value="date">Только дата</MenuItem>
                                <MenuItem value="datetime">Дата и время</MenuItem>
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
                              label="Выбрать только одно значение (иначе модель вернёт массив)"
                            />
                            {!field.listSingle && showAdvancedSchemaAttrs && (
                              <Stack direction={{ xs: "column", md: "row" }} spacing={1.25}>
                                <TextField
                                  label="Мин. элементов"
                                  value={field.listMinItems}
                                  onChange={(e) => updateSchemaField(field.id, { listMinItems: e.target.value })}
                                  sx={{ minWidth: 180 }}
                                />
                                <TextField
                                  label="Макс. элементов"
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
                  <Button variant="outlined" startIcon={<AddIcon />} onClick={addSchemaField}>
                    Добавить поле
                  </Button>
                  <Button
                    variant="outlined"
                    onClick={() => {
                      if (validateSchemaBuilder(schemaFields)) {
                        setActionToast({ message: "Схема валидна.", severity: "success" });
                      }
                    }}
                  >
                    Проверить схему
                  </Button>
                  <Button
                    variant="outlined"
                    startIcon={<RestartAltIcon />}
                    onClick={resetTemplateBuilder}
                  >
                    Сброс
                  </Button>
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

        {user.role !== "admin" && (
        <Card className="card">
          <CardContent>
            <Typography variant="h6">3. Обработка</Typography>
            {selectedLaunchReport && (
              <Typography variant="body2" color="text.secondary" sx={{ mt: 0.5 }}>
                Управление выбранным отчетом: {fmtDate(selectedLaunchReport.created_at)}
              </Typography>
            )}
            <Stack direction="row" spacing={1} sx={{ mt: 1 }} flexWrap="wrap" useFlexGap>
              <Button variant="contained" startIcon={<PlayArrowIcon />} onClick={startAnalysis}>
                Запустить новый анализ
              </Button>
              {(launchStatus === "running" || launchStatus === "queued") && (
                <Button variant="outlined" color="warning" startIcon={<PauseIcon />} onClick={pauseAnalysis}>
                  Пауза
                </Button>
              )}
              {launchStatus === "paused" && (
                <Button variant="outlined" color="success" startIcon={<PlayArrowIcon />} onClick={resumeAnalysis}>
                  Продолжить
                </Button>
              )}
              <Button
                variant="outlined"
                color="error"
                startIcon={<StopIcon />}
                onClick={cancelAnalysis}
                disabled={!launchJobId || !["running", "queued", "paused"].includes(launchStatus || "")}
              >
                Остановить
              </Button>
              {selectedLaunchReport && (selectedLaunchStatus === "running" || selectedLaunchStatus === "paused") && (
                <Button
                  variant="outlined"
                  startIcon={<DownloadIcon />}
                  onClick={() => openDownloadDialog(selectedLaunchReport, "partial")}
                >
                  Скачать промежуточный…
                </Button>
              )}
              {selectedLaunchReport && (selectedLaunchStatus === "failed" || selectedLaunchStatus === "canceled") && (
                <Button
                  variant="outlined"
                  color="secondary"
                  startIcon={<RestartAltIcon />}
                  onClick={() => retryReport(selectedLaunchReport.id, selectedLaunchReport.job_id)}
                  disabled={retryingReportId === selectedLaunchReport.id}
                >
                  Перезапустить
                </Button>
              )}
              {selectedLaunchReport && selectedLaunchStatus === "completed" && (Boolean(selectedLaunchReport.results_file) || Boolean(selectedLaunchReport.raw_file) || Boolean(selectedLaunchReport.uploaded_file_id)) && (
                <Button
                  variant="contained"
                  color="success"
                  startIcon={<DownloadIcon />}
                  onClick={() => openDownloadDialog(selectedLaunchReport, "completed")}
                >
                  Скачать…
                </Button>
              )}
              {job && !["running", "queued", "paused"].includes(job.status) && (
                <Button
                  variant="outlined"
                  color="error"
                  startIcon={<DeleteOutlineIcon />}
                  onClick={deleteCurrentFromLaunchBlock}
                >
                  Удалить
                </Button>
              )}
            </Stack>

            <Box sx={{ mt: 2 }}>
              {(() => {
                // Приоритет: для групповых отчётов всегда group_processed/total из
                // /api/reports snapshot (job.* из SSE идёт в строках — с лейблом «групп»
                // не совпадает). Для обычных — live job, иначе snapshot. current_step
                // с воркера «Обработка N/M» для групп скрываем — N/M в строках сбивает.
                const progress = computeReportProgress(selectedLaunchReport);
                const processedDisplay = progress.isGrouped
                  ? progress.processed
                  : (job ? job.processed : progress.processed);
                const totalDisplay = progress.isGrouped
                  ? progress.total
                  : (job ? job.total : progress.total);
                const percentDisplay = totalDisplay > 0
                  ? (processedDisplay / totalDisplay) * 100
                  : 0;
                const etaDisplay = job ? job.eta_seconds : (selectedLaunchReport?.eta_seconds ?? null);
                const rawStep = job?.current_step || selectedLaunchReport?.current_step || "";
                const stepDisplay = progress.isGrouped && /\d+\s*\/\s*\d+/.test(rawStep) ? "" : rawStep;
                const etaLabel = formatEta(etaDisplay);
                const hasAny = job || selectedLaunchReport;
                return (
                  <>
                    <LinearProgress variant="determinate" value={percentDisplay || 0} />
                    <Typography sx={{ mt: 1 }}>
                      {hasAny
                        ? `${processedDisplay.toLocaleString("ru-RU")} / ${totalDisplay.toLocaleString("ru-RU")} ${progress.unit} (${percentDisplay.toFixed(1)}%)${etaLabel ? ` · ${etaLabel}` : ""}`
                        : "Ожидание запуска"}
                    </Typography>
                    {stepDisplay && (
                      <Typography color="text.secondary">{stepDisplay}</Typography>
                    )}
                    {selectedLaunchReport?.status === "queued" && typeof selectedLaunchReport.queue_position === "number" && (
                      <Typography variant="caption" color="text.secondary" display="block">
                        {formatQueueHint(selectedLaunchReport.queue_position)}
                      </Typography>
                    )}
                    {isRunning && (
                      <Typography variant="caption" color="text.secondary">
                        В работе: {jobElapsedSec} с
                      </Typography>
                    )}
                  </>
                );
              })()}
            </Box>

          </CardContent>
        </Card>
        )}

        {activeResult?.summary && (
          <Box ref={resultsBlockRef}>
          <Card className="card">
            <CardContent>
              <Typography variant="h6">{activeResult.title}</Typography>

              <Stack direction={{ xs: "column", md: "row" }} spacing={2} sx={{ mt: 2 }} alignItems="stretch">
                {selectedLaunchReport?.group_by_column ? (
                  <Card variant="outlined" sx={{ flex: 1, minHeight: 110 }}>
                    <CardContent>
                      <Typography variant="overline" color="text.secondary">Всего групп</Typography>
                      <Typography variant="h4">
                        {selectedLaunchReport.group_processed ?? 0}
                        <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                          / {selectedLaunchReport.group_total ?? 0}
                        </Typography>
                      </Typography>
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
                        {activeResult.summary.total_rows.toLocaleString("ru-RU")} строк · колонка «{selectedLaunchReport.group_by_column}»
                      </Typography>
                    </CardContent>
                  </Card>
                ) : (
                  <Card variant="outlined" sx={{ flex: 1, minHeight: 110 }}>
                    <CardContent>
                      <Typography variant="overline" color="text.secondary">Всего строк</Typography>
                      <Typography variant="h4">{activeResult.summary.total_rows}</Typography>
                    </CardContent>
                  </Card>
                )}
                <Card variant="outlined" sx={{ flex: 1, minHeight: 110, borderColor: "success.main" }}>
                  <CardContent>
                    <Typography variant="overline" color="success.main">Успешно обработано</Typography>
                    <Typography variant="h4">
                      {activeResult.summary.success_rows.toLocaleString("ru-RU")}
                      {activeResult.summary.total_rows > 0 && (
                        <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                          ({Math.round((activeResult.summary.success_rows / activeResult.summary.total_rows) * 100)}%)
                        </Typography>
                      )}
                    </Typography>
                    {Boolean(selectedLaunchReport?.group_by_column) && (
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
                        строк внутри групп
                      </Typography>
                    )}
                  </CardContent>
                </Card>
                <Card variant="outlined" sx={{ flex: 1, minHeight: 110, borderColor: activeResult.summary.failed_rows > 0 ? "error.main" : undefined }}>
                  <CardContent>
                    <Typography variant="overline" color={activeResult.summary.failed_rows > 0 ? "error.main" : "text.secondary"}>С ошибками</Typography>
                    <Typography variant="h4">
                      {activeResult.summary.failed_rows.toLocaleString("ru-RU")}
                      {activeResult.summary.total_rows > 0 && activeResult.summary.failed_rows > 0 && (
                        <Typography component="span" variant="body2" color="text.secondary" sx={{ ml: 1 }}>
                          ({Math.round((activeResult.summary.failed_rows / activeResult.summary.total_rows) * 100)}%)
                        </Typography>
                      )}
                    </Typography>
                    {Boolean(selectedLaunchReport?.group_by_column) && (
                      <Typography variant="caption" color="text.secondary" display="block" sx={{ mt: 0.5 }}>
                        строк внутри групп
                      </Typography>
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
                <Alert severity="info" sx={{ mt: 3 }}>
                  Первые обработанные строки появятся здесь автоматически. Страница обновляется каждые 5 секунд.
                </Alert>
              )}

              {activeResult.previewRows && activeResult.previewRows.length > 0 && (
                <>
                  <Typography variant="subtitle1" sx={{ mt: 3 }}>
                    {activeResult.inProgress
                      ? `Промежуточный результат — первые ${activeResult.previewRows.length} обработанных строк`
                      : `Первые ${activeResult.previewRows.length} обработанных строк`}
                  </Typography>
                  <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 1 }}>
                    {activeResult.inProgress
                      ? "Обновляется каждые 5 секунд. Проверьте качество промпта на лету — если результат неудовлетворителен, можно поставить на паузу и начать новый отчёт."
                      : "Превью для быстрой проверки качества промпта. Полные данные — в XLSX-выгрузке."}
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
                          <TableCell>Предупреждения</TableCell>
                          <TableCell>Ошибка</TableCell>
                        </TableRow>
                      </TableHead>
                      <TableBody>
                        {activeResult.previewRows.map((item) => {
                          const renderValue = (value: unknown): string => {
                            if (value === null || value === undefined) return "-";
                            if (typeof value === "boolean") return value ? "да" : "нет";
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
        <Dialog open={releaseNotesOpen} onClose={() => setReleaseNotesOpen(false)} maxWidth="md" fullWidth>
          <DialogTitle sx={{ pb: 0.5 }}>
            Заметки о релизе
            <IconButton
              onClick={() => setReleaseNotesOpen(false)}
              sx={{ position: "absolute", right: 8, top: 8 }}
              aria-label="Закрыть"
            >
              <CloseIcon fontSize="small" />
            </IconButton>
          </DialogTitle>
          {releases.length > 0 && (
            <Tabs
              value={activeReleaseTab}
              onChange={(_, next) => setActiveReleaseTab(next)}
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
                        <Chip label="актуальная" size="small" color="primary" sx={{ height: 20, fontSize: "0.7rem" }} />
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
          <DialogContent dividers sx={{ minHeight: 320 }}>
            {releasesLoading && (
              <Stack alignItems="center" sx={{ py: 4 }}>
                <LinearProgress sx={{ width: "100%" }} />
                <Typography variant="caption" color="text.secondary" sx={{ mt: 1 }}>
                  Загружаем заметки…
                </Typography>
              </Stack>
            )}
            {!releasesLoading && releasesError && (
              <Typography variant="body2" color="error">
                Не удалось загрузить заметки: {releasesError}
              </Typography>
            )}
            {!releasesLoading && !releasesError && releases.length === 0 && (
              <Typography variant="body2" color="text.secondary">
                Заметки о релизе пока не опубликованы.
              </Typography>
            )}
            {!releasesLoading && !releasesError && releases.length > 0 && releases[activeReleaseTab] && (
              <Box
                sx={{
                  "& h1, & h2, & h3": { fontWeight: 700, mt: 2, mb: 1 },
                  "& h1": { fontSize: "1.3rem" },
                  "& h2": { fontSize: "1.2rem" },
                  "& h3": { fontSize: "1.05rem" },
                  "& p": { my: 1, lineHeight: 1.55 },
                  "& ul, & ol": { pl: 3, my: 1 },
                  "& li": { mb: 0.5 },
                  "& code": {
                    bgcolor: "action.hover",
                    px: 0.6,
                    py: 0.1,
                    borderRadius: 0.5,
                    fontSize: "0.88em",
                  },
                  "& strong": { fontWeight: 600 },
                  "& hr": { my: 2, border: 0, borderTop: 1, borderColor: "divider" },
                }}
              >
                <Typography variant="subtitle1" sx={{ fontWeight: 700, mb: 1 }}>
                  {releases[activeReleaseTab].title}
                </Typography>
                <ReactMarkdown>{releases[activeReleaseTab].content_md}</ReactMarkdown>
              </Box>
            )}
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setReleaseNotesOpen(false)} variant="contained">Закрыть</Button>
          </DialogActions>
        </Dialog>
        <Drawer
          anchor="right"
          open={examplesOpen}
          onClose={() => setExamplesOpen(false)}
          PaperProps={{ sx: { width: { xs: "100%", sm: 380 }, p: 2 } }}
        >
          <Stack direction="row" alignItems="center" justifyContent="space-between" sx={{ mb: 1.5 }}>
            <Typography variant="h6">Примеры файлов</Typography>
            <IconButton size="small" onClick={() => setExamplesOpen(false)} aria-label="Закрыть">
              <CloseIcon fontSize="small" />
            </IconButton>
          </Stack>
          <Typography variant="caption" color="text.secondary" sx={{ display: "block", mb: 2 }}>
            Клик по карточке — файл подставится как обычно загруженный, дальше настраиваете шаблон и запускаете.
          </Typography>
          {examplesLoading && (
            <Stack alignItems="center" sx={{ py: 3 }}>
              <LinearProgress sx={{ width: "100%" }} />
            </Stack>
          )}
          {!examplesLoading && examples.length === 0 && (
            <Typography variant="body2" color="text.secondary">
              В директории `examples/` нет доступных файлов.
            </Typography>
          )}
          <Stack spacing={1.5}>
            {examples.map((item) => (
              <Card key={item.name} variant="outlined">
                <CardContent sx={{ pb: 1 }}>
                  <Typography variant="subtitle2" sx={{ wordBreak: "break-word" }}>
                    {item.name}
                  </Typography>
                  <Typography variant="caption" color="text.secondary">
                    Размер: {formatBytes(item.size_bytes)}
                  </Typography>
                </CardContent>
                <Stack direction="row" justifyContent="flex-end" sx={{ px: 2, pb: 1.5 }}>
                  <Button
                    size="small"
                    variant="contained"
                    onClick={() => useExample(item.name)}
                    disabled={exampleLoadingName !== null}
                  >
                    {exampleLoadingName === item.name ? "Загрузка…" : "Использовать"}
                  </Button>
                </Stack>
              </Card>
            ))}
          </Stack>
        </Drawer>
        <Dialog open={Boolean(downloadDialog)} onClose={() => setDownloadDialog(null)} maxWidth="sm" fullWidth>
          <DialogTitle>Скачать отчёт</DialogTitle>
          <DialogContent>
            {downloadDialog && (
              <Stack spacing={2} sx={{ mt: 1 }}>
                {(downloadDialog.status === "running" || downloadDialog.status === "paused") && (
                  <Alert severity="info">
                    Отчёт ещё обрабатывается. Файл соберётся на лету из уже готовых строк.
                  </Alert>
                )}
                <FormControl>
                  <Typography variant="subtitle2" sx={{ mb: 1 }}>Формат</Typography>
                  <RadioGroup
                    value={downloadDialog.format}
                    onChange={(_, value) => setDownloadDialog((prev) => prev ? { ...prev, format: value as "xlsx" | "raw" | "source" } : prev)}
                  >
                    {downloadDialog.hasResults && (
                      <FormControlLabel value="xlsx" control={<Radio />} label="XLSX (таблица с результатом)" />
                    )}
                    {downloadDialog.hasRaw && (
                      <FormControlLabel value="raw" control={<Radio />} label="Сырой JSON (ответы модели + метаданные)" />
                    )}
                    {downloadDialog.hasSource && (
                      <FormControlLabel value="source" control={<Radio />} label="Исходный файл пользователя" />
                    )}
                  </RadioGroup>
                </FormControl>
                <TextField
                  label="Имя файла"
                  value={downloadDialog.filename}
                  onChange={(e) => setDownloadDialog((prev) => prev ? { ...prev, filename: e.target.value } : prev)}
                  helperText="Без расширения — оно добавится автоматически по выбранному формату. Недопустимые символы заменятся на _."
                  fullWidth
                />
              </Stack>
            )}
          </DialogContent>
          <DialogActions>
            <Button onClick={() => setDownloadDialog(null)}>Отмена</Button>
            <Button variant="contained" startIcon={<DownloadIcon />} onClick={submitDownload}>
              Скачать
            </Button>
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
    </Box>
  );
}

export default App;


