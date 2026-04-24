"""Юнит-тесты для `remove_job_from_queue` / `remove_inspect_from_queue` (IDEA-07 follow-up).

При отмене queued-задачи payload должен уходить из Redis-LIST, иначе соседние
задачи продолжают видеть его в своих `queue_position` и показывают лишнюю
позицию в UI (баг на скриншоте: после отмены одного queued-отчёта другой
продолжает видеть «Перед вами: 1»).

Запуск:
    cd backend && pytest tests/unit/test_queue_remove.py -v
"""
from __future__ import annotations

import json
from unittest.mock import MagicMock, patch


def _mock_redis_with_list(items: list[str], lrem_return: int = 1) -> MagicMock:
    client = MagicMock()
    client.lrange.return_value = list(items)
    client.lrem.return_value = lrem_return
    client.delete.return_value = 1
    return client


def test_remove_middle_payload_returns_1() -> None:
    from app.queue import remove_job_from_queue

    items = [
        json.dumps({"job_id": "first"}, ensure_ascii=False),
        json.dumps({"job_id": "target"}, ensure_ascii=False),
        json.dumps({"job_id": "third"}, ensure_ascii=False),
    ]
    client = _mock_redis_with_list(items)
    with patch("app.queue._redis", return_value=client):
        removed = remove_job_from_queue("target")
    assert removed == 1
    # LREM вызван ровно на ту полезную нагрузку
    client.lrem.assert_called_once()
    args = client.lrem.call_args.args
    assert json.loads(args[2]) == {"job_id": "target"}


def test_remove_not_in_queue_returns_zero_but_deletes_marker() -> None:
    """Если задача уже взята воркером (её нет в LIST'е) — LREM не делаем,
    но dedup-маркер всё равно чистим (на случай если он остался висеть)."""
    from app.queue import remove_job_from_queue

    items = [json.dumps({"job_id": "other"}, ensure_ascii=False)]
    client = _mock_redis_with_list(items)
    with patch("app.queue._redis", return_value=client):
        removed = remove_job_from_queue("missing")
    assert removed == 0
    client.lrem.assert_not_called()
    client.delete.assert_called_once()


def test_remove_empty_id_is_noop() -> None:
    """Пустой id — ранний выход без обращения к Redis."""
    from app.queue import remove_job_from_queue

    client = _mock_redis_with_list([])
    with patch("app.queue._redis", return_value=client):
        assert remove_job_from_queue("") == 0
    client.lrange.assert_not_called()
    client.lrem.assert_not_called()
    client.delete.assert_not_called()


def test_remove_duplicate_payloads_counted() -> None:
    """В редком случае дубля payload'а LREM снимается по всем вхождениям."""
    from app.queue import remove_job_from_queue

    payload = json.dumps({"job_id": "dup"}, ensure_ascii=False)
    items = [payload, payload]
    client = _mock_redis_with_list(items, lrem_return=1)
    with patch("app.queue._redis", return_value=client):
        removed = remove_job_from_queue("dup")
    # LREM вызывается по каждому найденному элементу, каждый удаляет 1.
    assert removed == 2
    assert client.lrem.call_count == 2


def test_remove_malformed_payload_skipped() -> None:
    """Мусорные элементы (не JSON) не мешают найти таргет дальше по списку."""
    from app.queue import remove_job_from_queue

    items = [
        "not a json",
        json.dumps({"job_id": "target"}, ensure_ascii=False),
    ]
    client = _mock_redis_with_list(items)
    with patch("app.queue._redis", return_value=client):
        removed = remove_job_from_queue("target")
    assert removed == 1


def test_remove_inspect_uses_file_id() -> None:
    """remove_inspect_from_queue матчит по file_id и чистит INSPECT-маркер."""
    from app.queue import INSPECT_QUEUE_KEY, remove_inspect_from_queue

    items = [json.dumps({"file_id": "FILE-1"}, ensure_ascii=False)]
    client = _mock_redis_with_list(items)
    with patch("app.queue._redis", return_value=client):
        removed = remove_inspect_from_queue("FILE-1")
    assert removed == 1
    # LREM бил в inspect-очередь, а не в analysis-очередь
    first_call = client.lrem.call_args.args
    assert first_call[0] == INSPECT_QUEUE_KEY


def test_remove_resilient_to_redis_errors() -> None:
    """Падение Redis не пробрасывается из функции — безопасно вызывать при cancel."""
    from app.queue import remove_job_from_queue

    client = MagicMock()
    client.lrange.side_effect = Exception("boom")
    client.delete.return_value = 0
    with patch("app.queue._redis", return_value=client):
        removed = remove_job_from_queue("any")
    # lrange упал → 0, маркер всё равно пытаемся удалить
    assert removed == 0
    client.delete.assert_called_once()
