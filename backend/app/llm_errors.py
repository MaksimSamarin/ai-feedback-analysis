"""Понятные причины ошибок ответа модели с сохранением диагностики."""
import json
import re
import httpx

_SHOWN_VALUE_LIMIT = 200


class EnumMismatch(ValueError):
    """Значение поля не из справочника; помнит ответ модели именно по этому полю."""

    def __init__(self, message: str, *, value):
        super().__init__(message)
        self.value = value


def _shown_value(value) -> str:
    text = value if isinstance(value, str) else json.dumps(value, ensure_ascii=False)
    return text if len(text) <= _SHOWN_VALUE_LIMIT else text[:_SHOWN_VALUE_LIMIT] + "…"


def explain_error(exc):
    raw = str(exc)
    lower = raw.lower()
    field_match = re.search(r"Поле '([^']+)'", raw)
    field = field_match.group(1) if field_match else None
    if isinstance(exc, EnumMismatch) and field and (exc.value is None or str(exc.value).strip() == ""):
        code, message = "required_empty", f"Модель оставила поле «{field}» пустым, а нужно значение из разрешённого списка."
    elif "одним из:" in raw and field:
        code = "outside_enum"
        if isinstance(exc, EnumMismatch):
            message = f"Модель вернула в поле «{field}» значение «{_shown_value(exc.value)}» — его нет в разрешённом списке."
        else:
            message = f"Значение поля «{field}» отсутствует в разрешённом списке."
    elif "обязательное поле:" in raw:
        code, message = "required_empty", "Не заполнено обязательное поле «" + raw.split("обязательное поле:", 1)[1].strip() + "»."
    elif "context" in lower or "контекст" in lower:
        code, message = "context_length", "Превышен контекст модели. Уменьшите объём данных в запросе."
    elif "обрезан" in lower or "finish_reason=length" in lower:
        code, message = "truncated", "Ответ модели обрезан. Уменьшите объём данных или увеличьте лимит ответа."
    elif isinstance(exc, (TimeoutError, httpx.TimeoutException)) or "timed out" in lower:
        code, message = "timeout", "Провайдер не ответил вовремя. Повторите обработку позже."
    elif isinstance(exc, httpx.HTTPStatusError):
        status = exc.response.status_code
        code = "provider_error"
        message = "Провайдер ограничил частоту запросов. Повторите позже." if status == 429 else f"Ошибка провайдера ({status}). Повторите позже."
    elif isinstance(exc, json.JSONDecodeError) or "не json" in lower or "json" in lower and ("валидн" in lower or "объект" in lower):
        code, message = "invalid_json", "Модель вернула ответ, который не удалось разобрать как JSON."
    elif field:
        code, message = "field_format", f"Неверный формат поля «{field}». {raw}"
    else:
        code, message = "invalid_response", "Не удалось обработать ответ модели. Повторите обработку или уточните настройки."
    return {"code": code, "message": message, "diagnostic": raw[:4000]}
