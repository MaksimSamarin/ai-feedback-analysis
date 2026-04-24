from __future__ import annotations

import json
import os
import uuid
from typing import Any

try:
    from redis import Redis
except Exception:  # pragma: no cover
    Redis = None  # type: ignore[assignment]

from app.db import get_redis_client

QUEUE_KEY = os.getenv("REDIS_QUEUE_KEY", "review_analyzer:jobs")
INSPECT_QUEUE_KEY = os.getenv("REDIS_INSPECT_QUEUE_KEY", "review_analyzer:file_inspect_jobs")
JOB_QUEUE_DEDUP_TTL_SEC = max(60, int(os.getenv("JOB_QUEUE_DEDUP_TTL_SEC", "3600")))
RUNNING_LEASE_TTL_SEC = max(15, int(os.getenv("WORKER_RUNNING_LEASE_TTL_SEC", "30")))


def _queued_marker_key(queue_key: str, dedup_id: str) -> str:
    return f"{queue_key}:queued:{dedup_id}"


def _running_lease_key(job_id: str) -> str:
    return f"{QUEUE_KEY}:running:{job_id}"


def _redis() -> Redis:
    """Возвращает общий Redis-клиент из `db.get_redis_client()` (BUG-04).

    Очередь не может работать без Redis, поэтому при недоступности клиента
    поднимаем RuntimeError — это явная ошибка конфигурации.
    Единый пул для всего процесса (раньше `queue.py` и `db.py` держали каждый свой).
    """
    client = get_redis_client()
    if client is None:
        raise RuntimeError(
            "Redis недоступен — проверьте переменную REDIS_URL и установку пакета redis"
        )
    return client


def enqueue_job(payload: dict[str, Any]) -> bool:
    client = _redis()
    job_id = str(payload.get("job_id") or "").strip()
    if job_id:
        marker_set = client.set(_queued_marker_key(QUEUE_KEY, job_id), "1", nx=True, ex=JOB_QUEUE_DEDUP_TTL_SEC)
        if not marker_set:
            return False
    client.rpush(QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
    return True


def enqueue_inspect_job(payload: dict[str, Any]) -> bool:
    client = _redis()
    dedup_id = str(payload.get("file_id") or payload.get("job_id") or "").strip()
    if dedup_id:
        marker_set = client.set(_queued_marker_key(INSPECT_QUEUE_KEY, dedup_id), "1", nx=True, ex=JOB_QUEUE_DEDUP_TTL_SEC)
        if not marker_set:
            return False
    client.rpush(INSPECT_QUEUE_KEY, json.dumps(payload, ensure_ascii=False))
    return True


def requeue_after_transient_error(payload: dict[str, Any], *, kind: str) -> bool:
    """Возвращает payload в соответствующую очередь без проверки дедупа (BUG-15).

    Используется в worker main-loop после транзиентной ошибки БД
    (`psycopg.OperationalError` / `AdminShutdown`): задача уже была достана из
    очереди, её нужно вернуть чтобы она не потерялась, а dedup-маркер
    (от изначального enqueue) ещё жив — поэтому обычный enqueue_*_job
    вернёт False. Здесь делаем прямой rpush.

    `kind` = "analysis" (в QUEUE_KEY) или "file_inspect" (в INSPECT_QUEUE_KEY).
    """
    client = _redis()
    if kind == "file_inspect":
        target = INSPECT_QUEUE_KEY
    else:
        target = QUEUE_KEY
    try:
        client.rpush(target, json.dumps(payload, ensure_ascii=False))
        return True
    except Exception:
        return False


def dequeue_job(timeout_sec: int = 5) -> dict[str, Any] | None:
    client = _redis()
    item = client.blpop(QUEUE_KEY, timeout=max(1, timeout_sec))
    if not item:
        return None
    _, value = item
    parsed = json.loads(value)
    if isinstance(parsed, dict):
        job_id = str(parsed.get("job_id") or "").strip()
        if job_id:
            client.delete(_queued_marker_key(QUEUE_KEY, job_id))
        return parsed
    return None


def dequeue_inspect_job(timeout_sec: int = 5) -> dict[str, Any] | None:
    client = _redis()
    item = client.blpop(INSPECT_QUEUE_KEY, timeout=max(1, timeout_sec))
    if not item:
        return None
    _, value = item
    parsed = json.loads(value)
    if isinstance(parsed, dict):
        dedup_id = str(parsed.get("file_id") or parsed.get("job_id") or "").strip()
        if dedup_id:
            client.delete(_queued_marker_key(INSPECT_QUEUE_KEY, dedup_id))
        return parsed
    return None


def has_running_lease(job_id: str) -> bool:
    clean = str(job_id or "").strip()
    if not clean:
        return False
    client = _redis()
    return bool(client.exists(_running_lease_key(clean)))


def has_queued_marker(job_id: str) -> bool:
    clean = str(job_id or "").strip()
    if not clean:
        return False
    client = _redis()
    return bool(client.exists(_queued_marker_key(QUEUE_KEY, clean)))


def claim_running_lease(job_id: str, ttl_sec: int | None = None) -> bool:
    clean = str(job_id or "").strip()
    if not clean:
        return False
    ttl = max(5, int(ttl_sec or RUNNING_LEASE_TTL_SEC))
    client = _redis()
    ok = client.set(_running_lease_key(clean), "1", nx=True, ex=ttl)
    return bool(ok)


def touch_running_lease(job_id: str, ttl_sec: int | None = None) -> bool:
    clean = str(job_id or "").strip()
    if not clean:
        return False
    ttl = max(5, int(ttl_sec or RUNNING_LEASE_TTL_SEC))
    client = _redis()
    ok = client.set(_running_lease_key(clean), "1", xx=True, ex=ttl)
    return bool(ok)


def release_running_lease(job_id: str) -> None:
    clean = str(job_id or "").strip()
    if not clean:
        return
    client = _redis()
    client.delete(_running_lease_key(clean))


def acquire_lock(lock_key: str, ttl_sec: int = 30) -> str | None:
    client = _redis()
    token = uuid.uuid4().hex
    ok = client.set(lock_key, token, nx=True, ex=max(1, int(ttl_sec)))
    return token if ok else None


def release_lock(lock_key: str, token: str) -> None:
    client = _redis()
    script = """
if redis.call('get', KEYS[1]) == ARGV[1] then
  return redis.call('del', KEYS[1])
else
  return 0
end
"""
    client.eval(script, 1, lock_key, token)


def get_queue_depth() -> int:
    client = _redis()
    return int(client.llen(QUEUE_KEY) or 0)


def _find_position_by_id(queue_key: str, id_field: str, payload_id: str) -> int | None:
    """Возвращает 0-based позицию задачи в Redis-LIST по совпадению `id_field` в
    JSON-payload'е. None, если задача не найдена (уже взята воркером / отсутствует).

    На практике длина очереди — десятки задач, перебор через `LRANGE` достаточен;
    сложная индексация не нужна.
    """
    clean = str(payload_id or "").strip()
    if not clean:
        return None
    client = _redis()
    try:
        items = client.lrange(queue_key, 0, -1) or []
    except Exception:
        return None
    for idx, raw in enumerate(items):
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        if str(parsed.get(id_field) or "").strip() == clean:
            return idx
    return None


def get_job_queue_position(job_id: str) -> int | None:
    """Позиция job'а в очереди анализа (0-based). None — если задача не в очереди."""
    return _find_position_by_id(QUEUE_KEY, "job_id", job_id)


def get_inspect_queue_position(file_id: str) -> int | None:
    """Позиция inspect-задачи в очереди подготовки файла (0-based). None — если не в очереди."""
    return _find_position_by_id(INSPECT_QUEUE_KEY, "file_id", file_id)


def _remove_by_id(queue_key: str, id_field: str, payload_id: str) -> int:
    """Удаляет все payload'ы с указанным `id_field` из Redis-LIST.

    Возвращает количество удалённых элементов. Нужно при отмене задачи:
    иначе отменённая запись продолжает занимать место в очереди и искажает
    `queue_position` у соседних задач, хотя воркер её всё равно пропустит
    по статусу canceled в БД.
    """
    clean = str(payload_id or "").strip()
    if not clean:
        return 0
    client = _redis()
    try:
        items = client.lrange(queue_key, 0, -1) or []
    except Exception:
        return 0
    removed_total = 0
    for raw in items:
        try:
            parsed = json.loads(raw)
        except Exception:
            continue
        if not isinstance(parsed, dict):
            continue
        if str(parsed.get(id_field) or "").strip() != clean:
            continue
        try:
            removed = int(client.lrem(queue_key, 0, raw) or 0)
        except Exception:
            removed = 0
        removed_total += removed
    return removed_total


def remove_job_from_queue(job_id: str) -> int:
    """Удаляет payload job'а из QUEUE_KEY + чистит dedup-маркер.

    Вызывается при отмене отчёта. Возвращает количество удалённых записей
    (обычно 0 — если задачу уже взял воркер — или 1).
    """
    removed = _remove_by_id(QUEUE_KEY, "job_id", job_id)
    clean = str(job_id or "").strip()
    if clean:
        try:
            _redis().delete(_queued_marker_key(QUEUE_KEY, clean))
        except Exception:
            pass
    return removed


def remove_inspect_from_queue(file_id: str) -> int:
    """Удаляет inspect-payload из INSPECT_QUEUE_KEY + чистит dedup-маркер."""
    removed = _remove_by_id(INSPECT_QUEUE_KEY, "file_id", file_id)
    clean = str(file_id or "").strip()
    if clean:
        try:
            _redis().delete(_queued_marker_key(INSPECT_QUEUE_KEY, clean))
        except Exception:
            pass
    return removed
