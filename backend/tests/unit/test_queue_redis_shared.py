"""Юнит-тесты на единый Redis-пул для `queue.py` и `db.py` (BUG-04).

После фикса `queue._redis()` делегирует к `db.get_redis_client()` — один пул на
процесс вместо двух независимых.

Запуск:
    cd backend && pytest tests/unit/test_queue_redis_shared.py -v
"""

from __future__ import annotations

from unittest.mock import MagicMock

import pytest


def test_queue_redis_shares_instance_with_db(monkeypatch: pytest.MonkeyPatch) -> None:
    """`queue._redis()` и `db.get_redis_client()` должны возвращать ОДИН объект.

    Тождественность (`is`) гарантирует что пул соединений один, а не два разных.
    Если кто-то в будущем отменит делегирование — тест упадёт.

    Патчим `app.queue.get_redis_client` (а не `app.db.get_redis_client`), потому что
    `queue.py` делает `from app.db import get_redis_client` — имя скопировано в namespace
    queue, при вызове Python ищет его там.
    """
    fake_client = MagicMock(name="shared_redis_client")
    monkeypatch.setattr("app.queue.get_redis_client", lambda: fake_client)

    from app import queue as queue_module

    client_from_queue = queue_module._redis()

    # Прямой вызов через то же имя, куда мы патчили, — должен вернуть тот же объект
    client_from_patched_ref = queue_module.get_redis_client()

    assert client_from_queue is client_from_patched_ref, (
        "queue._redis() не делегирует к get_redis_client — пулы Redis снова разделились "
        "(регрессия BUG-04)"
    )
    assert client_from_queue is fake_client, (
        "queue._redis() вернул не тот объект, который отдавал подменённый get_redis_client"
    )


def test_queue_redis_raises_when_client_unavailable(monkeypatch: pytest.MonkeyPatch) -> None:
    """Если `get_redis_client()` вернул None — `queue._redis()` должен поднять
    RuntimeError с понятным сообщением.

    Очередь не может работать без Redis, поэтому тихий возврат None недопустим —
    нужна явная ошибка конфигурации.
    """
    # Патчим импортированное имя в app.queue — именно его вызывает _redis()
    monkeypatch.setattr("app.queue.get_redis_client", lambda: None)

    from app import queue as queue_module

    with pytest.raises(RuntimeError, match="Redis недоступен"):
        queue_module._redis()
