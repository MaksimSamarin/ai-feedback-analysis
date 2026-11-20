"""Контроль остатка без блокировки параллельных запросов к модели."""
from __future__ import annotations

import asyncio
from dataclasses import dataclass, field
from datetime import datetime
import os
import time
import uuid

from fastapi import HTTPException

from app import access, db

REFRESH_THRESHOLD_TOKENS = max(0, int(os.getenv("LIMIT_REFRESH_THRESHOLD_TOKENS", "1024")))
ESTIMATED_REQUEST_TOKENS = max(1, int(os.getenv("LIMIT_ESTIMATED_REQUEST_TOKENS", "256")))

_PUBLISH = """
redis.call('HSETNX', KEYS[2], 'epoch', ARGV[1])
local delta = tonumber(ARGV[2])
if delta ~= 0 then
  for i = 3, 6 do redis.call('HINCRBY', KEYS[2], ARGV[i], delta) end
end
return redis.call('INCR', KEYS[1])
"""
_READ = """
redis.call('HSETNX', KEYS[2], 'epoch', ARGV[1])
return {redis.call('GET', KEYS[1]) or '0', redis.call('HGET', KEYS[2], 'epoch'),
        redis.call('HGET', KEYS[2], ARGV[2]) or '0',
        redis.call('HGET', KEYS[2], ARGV[3]) or '0'}
"""


def _month(created_at):
    return datetime.fromisoformat(created_at).astimezone(access.MOSCOW).strftime("%Y-%m")


def publish_usage(client, group_id, user_id, created_at, delta):
    month = _month(created_at) if created_at else ""
    client.eval(
        _PUBLISH, 2, f"quota:version:{group_id}", f"quota:deltas:{group_id}",
        uuid.uuid4().hex, delta, "all", f"month:{month}",
        f"user:{user_id}:all", f"user:{user_id}:month:{month}",
    )


def load_settings(user_id, group_id):
    with db.get_conn() as conn:
        row = conn.execute(
            """SELECT g.id,g.name,g.token_limit,g.limit_period,g.is_active,
                      m.user_id,m.limit_mode,m.personal_token_limit
               FROM access_groups g LEFT JOIN access_group_members m
                 ON m.group_id=g.id AND m.user_id=? WHERE g.id=?""",
            (user_id, group_id),
        ).fetchone()
    if not row or not row["is_active"] or row["user_id"] is None:
        raise HTTPException(403, {"code": "no_access", "message": "Доступ к группе отключён"})
    return dict(row)


def count_usage(user_id, group, created_at):
    start, end = access.period_bounds(group["limit_period"], datetime.fromisoformat(created_at))
    sql = """SELECT COALESCE(SUM(total_tokens),0) AS group_used,
             COALESCE(SUM(total_tokens) FILTER (WHERE user_id=?),0) AS personal_used
             FROM reports WHERE access_group_id=?"""
    args = [user_id, group["id"]]
    if start:
        sql += " AND created_at>=? AND created_at<?"
        args.extend([start, end])
    with db.get_conn() as conn:
        row = conn.execute(sql, args).fetchone()
    return int(row["group_used"]), int(row["personal_used"]), end


@dataclass
class QuotaState:
    user_id: int
    group_id: int
    created_at: str
    parallelism: int
    settings: dict | None = None
    average_tokens: float = ESTIMATED_REQUEST_TOKENS
    samples: int = 0
    _settings_at: float = 0
    _basis: tuple | None = None
    _lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    def _counters(self):
        client = db.get_redis_client()
        if client is None:
            raise HTTPException(503, "Недоступна проверка лимита")
        period = "all" if self.settings["limit_period"] == "none" else f"month:{_month(self.created_at)}"
        try:
            values = client.eval(
                _READ, 2, f"quota:version:{self.group_id}", f"quota:deltas:{self.group_id}",
                uuid.uuid4().hex, period, f"user:{self.user_id}:{period}",
            )
        except Exception as exc:
            raise HTTPException(503, "Недоступна проверка лимита") from exc
        version, epoch, group_delta, personal_delta = values
        if isinstance(epoch, bytes):
            epoch = epoch.decode()
        return int(version), epoch, int(group_delta), int(personal_delta)

    async def _refresh(self):
        # Счётчики читаются до SUM: запись между этими чтениями может только
        # временно завысить оценку, но не разрешить лишний расход.
        counters = await asyncio.to_thread(self._counters)
        used, personal, reset = await asyncio.to_thread(
            count_usage, self.user_id, self.settings, self.created_at,
        )
        self._basis = (counters, used, personal, reset)

    def _remaining(self, counters):
        previous, used, personal, reset = self._basis
        used += counters[2] - previous[2]
        personal += counters[3] - previous[3]
        limits = access.remaining(
            self.settings["token_limit"], used, self.settings["limit_mode"],
            self.settings["personal_token_limit"], personal,
        )
        return limits, used, personal, reset

    async def before_request(self):
        async with self._lock:
            now = time.monotonic()
            if self.settings is None or now - self._settings_at >= access.LIMIT_USAGE_CACHE_TTL_SEC:
                settings = await asyncio.to_thread(load_settings, self.user_id, self.group_id)
                if self.settings is not None and settings["limit_period"] != self.settings["limit_period"]:
                    self._basis = None
                self.settings = settings
                self._settings_at = now
            if self.settings["token_limit"] is None and self.settings["limit_mode"] == "group":
                return None
            counters = await asyncio.to_thread(self._counters)
            refreshed = False
            if self._basis is None or counters[1] != self._basis[0][1] or counters[0] < self._basis[0][0]:
                await self._refresh()
                counters = await asyncio.to_thread(self._counters)
                refreshed = True
            limits, used, personal, reset = self._remaining(counters)
            threshold = max(REFRESH_THRESHOLD_TOKENS, self.parallelism * self.average_tokens * 2)
            if not refreshed and limits["remaining"] <= threshold:
                await self._refresh()
                counters = await asyncio.to_thread(self._counters)
                limits, used, personal, reset = self._remaining(counters)
            if limits["group_remaining"] is not None and limits["group_remaining"] <= 0:
                raise access.LimitExhausted("групповой", used, self.settings["token_limit"], reset,
                    {'id': self.group_id, 'name': self.settings.get('name', str(self.group_id))})
            if limits["personal_remaining"] is not None and limits["personal_remaining"] <= 0:
                raise access.LimitExhausted("личный", personal, self.settings["personal_token_limit"], reset,
                    {'id': self.group_id, 'name': self.settings.get('name', str(self.group_id))})
            return limits["remaining"]

    async def after_write(self, tokens):
        if tokens > 0:
            self.samples += 1
            self.average_tokens += (tokens - self.average_tokens) / self.samples
        return await self.before_request()
