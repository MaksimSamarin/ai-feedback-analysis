from copy import deepcopy

import pytest
from fastapi import HTTPException

from app import quota


def settings():
    return {"id": 4, "limit_period": "month", "token_limit": 10000000,
            "limit_mode": "group", "personal_token_limit": None}


@pytest.mark.asyncio
async def test_large_remaining_does_not_sum_usage_per_row(monkeypatch):
    group = settings()
    ledger = [0, "epoch", 0, 0]
    counts = []
    monkeypatch.setattr(quota, "load_settings", lambda *args: deepcopy(group))
    monkeypatch.setattr(quota.QuotaState, "_counters", lambda self: tuple(ledger))
    def count(*args):
        counts.append(1)
        return ledger[2], ledger[3], None
    monkeypatch.setattr(quota, "count_usage", count)
    state = quota.QuotaState(2, 4, "2026-11-08T00:00:00+00:00", 4)
    for _ in range(100):
        state._settings_at = 0
        assert await state.before_request() > 0
        ledger[0] += 1
        ledger[2] += 100
        ledger[3] += 100
        await state.after_write(100)
    assert len(counts) == 1


@pytest.mark.asyncio
async def test_other_report_spend_and_near_limit_are_seen(monkeypatch):
    group = settings()
    group["token_limit"] = 100
    ledger = [0, "epoch", 0, 0]
    monkeypatch.setattr(quota, "load_settings", lambda *args: deepcopy(group))
    monkeypatch.setattr(quota.QuotaState, "_counters", lambda self: tuple(ledger))
    monkeypatch.setattr(quota, "count_usage", lambda *args: (ledger[2], ledger[3], None))
    first = quota.QuotaState(2, 4, "2026-11-08T00:00:00+00:00", 4)
    other = quota.QuotaState(3, 4, first.created_at, 4)
    assert await first.before_request() == await other.before_request() == 100
    ledger[:] = [1, "epoch", 120, 0]
    with pytest.raises(HTTPException) as exc:
        await first.before_request()
    assert exc.value.detail["code"] == "limit_exhausted"


@pytest.mark.asyncio
async def test_ledger_restart_refreshes_database_basis(monkeypatch):
    ledger = [10, "old", 100, 100]
    monkeypatch.setattr(quota, "load_settings", lambda *args: settings())
    monkeypatch.setattr(quota.QuotaState, "_counters", lambda self: tuple(ledger))
    counts = []
    def count(*args):
        counts.append(1)
        return 500, 100, None
    monkeypatch.setattr(quota, "count_usage", count)
    state = quota.QuotaState(2, 4, "2026-11-08T00:00:00+00:00", 4)
    assert await state.before_request() == 9999500
    ledger[:] = [0, "new", 0, 0]
    assert await state.before_request() == 9999500
    assert len(counts) == 2
