"""Ожидание при ответе провайдера 429 Too Many Requests.

Лимит частоты считается провайдером за окно (обычно минуту), поэтому короткие
повторы через 0,5–1 с гарантированно попадают в то же окно. Ждём столько, сколько
просит Retry-After, а без заголовка — с нарастающей паузой. Ожидания 429 не
расходуют попытки LLM_RETRIES, отведённые на ошибки ответа модели.
"""
from __future__ import annotations

import asyncio
import random
import time
from email.utils import parsedate_to_datetime

import httpx

from app.config import LLM_RATE_LIMIT_MAX_WAIT_SEC

_BASE_DELAY_SEC = 5.0
# До какого момента (time.monotonic) провайдер просил не слать запросы. Общее для
# всех строк процесса: если одна строка получила 429, параллельные не долбят
# провайдера, а ждут вместе с ней.
_cooldown_until: dict[str, float] = {}


def _retry_after_seconds(response: httpx.Response) -> float | None:
    headers = response.headers
    raw_ms = headers.get("retry-after-ms")
    if raw_ms:
        try:
            return max(0.0, float(raw_ms) / 1000)
        except ValueError:
            pass
    raw = (headers.get("retry-after") or "").strip()
    if not raw:
        return None
    try:
        return max(0.0, float(raw))
    except ValueError:
        pass
    try:
        moment = parsedate_to_datetime(raw)
    except (TypeError, ValueError):
        return None
    if moment is None:
        return None
    return max(0.0, moment.timestamp() - time.time())


def _is_exhausted_quota(response: httpx.Response) -> bool:
    """429 из-за исчерпанного баланса/квоты ключа ожиданием не лечится."""
    try:
        body = response.json()
    except Exception:
        return False
    error = body.get("error") if isinstance(body, dict) else None
    if not isinstance(error, dict):
        return False
    return str(error.get("code") or error.get("type") or "") == "insufficient_quota"


def rate_limit_delay(exc: BaseException, hit: int) -> float | None:
    """Сколько ждать перед повтором; None — это не 429 или повтор бесполезен.

    hit — номер ожидания для этой строки, начиная с 0.
    """
    if not isinstance(exc, httpx.HTTPStatusError) or exc.response is None:
        return None
    if exc.response.status_code != 429 or _is_exhausted_quota(exc.response):
        return None
    requested = _retry_after_seconds(exc.response)
    if requested is not None:
        return min(max(requested, 1.0), LLM_RATE_LIMIT_MAX_WAIT_SEC)
    backoff = min(_BASE_DELAY_SEC * (2 ** hit), LLM_RATE_LIMIT_MAX_WAIT_SEC)
    return backoff * random.uniform(0.8, 1.0)


def start_cooldown(key: str, delay: float) -> None:
    until = time.monotonic() + delay
    if until > _cooldown_until.get(key, 0.0):
        _cooldown_until[key] = until


async def wait_cooldown(key: str) -> float:
    """Ждёт окончания паузы провайдера, если она объявлена; возвращает ожидание, с."""
    remaining = _cooldown_until.get(key, 0.0) - time.monotonic()
    if remaining <= 0:
        return 0.0
    await asyncio.sleep(remaining)
    return remaining
