"""Юнит-тесты на удаление Redis L1-кэша LLM (v2.1.2).

Регрессия на BUG-19: до v2.1.2 `put_cached_analysis` дублировал кэш LLM-ответов
в Redis с ключами `llm_cache:*` БЕЗ TTL и БЕЗ enforce-лимита. За неделю
накапливалось ~1M ключей (~1 ГБ), Redis упирался в mem_limit=1g и убивался
OOM-killer'ом — пользователи теряли отчёты с ошибкой
«Redis is loading the dataset in memory».

В v2.1.2 L1 Redis-кэш полностью удалён из трёх функций cache-API; остаётся
только Postgres-кэш (там есть `_enforce_llm_cache_limit`). Дополнительно
при старте worker'а запускается одноразовая миграция
`cleanup_legacy_llm_cache_redis_keys`, которая через SCAN+UNLINK удаляет
накопленные legacy-ключи.

Запуск:
    cd backend && pytest tests/unit/test_llm_cache_redis_removed.py -v
"""
from __future__ import annotations

from pathlib import Path


def _db_source() -> str:
    here = Path(__file__).resolve()
    src_path = here.parent.parent.parent / "app" / "db.py"
    return src_path.read_text(encoding="utf-8")


def _extract_function(src: str, name: str) -> str:
    start = src.find(f"def {name}(")
    assert start > 0, f"Не нашли функцию {name} в db.py"
    next_def = src.find("\ndef ", start + 1)
    return src[start: next_def if next_def > 0 else len(src)]


def test_get_cached_analysis_no_redis_calls() -> None:
    """get_cached_analysis не должна обращаться к Redis."""
    body = _extract_function(_db_source(), "get_cached_analysis")
    assert 'redis_client.get(f"llm_cache:' not in body, (
        "L1 Redis-чтение должно быть удалено — Postgres единственный источник"
    )
    assert "get_redis_client()" not in body, (
        "Redis-клиент не должен инициализироваться в get_cached_analysis"
    )


def test_put_cached_analysis_no_redis_writes() -> None:
    """put_cached_analysis не должна писать в Redis."""
    body = _extract_function(_db_source(), "put_cached_analysis")
    assert 'redis_client.set(' not in body, (
        "L1 Redis-запись должна быть удалена — это и был источник OOM-kill"
    )
    assert 'llm_cache:' not in body, (
        "Префикс llm_cache не должен использоваться в Redis после v2.1.2"
    )
    # Постгресная запись остаётся.
    assert "INSERT INTO llm_cache" in body


def test_delete_cached_analysis_no_redis_delete() -> None:
    """delete_cached_analysis больше не дёргает Redis (нечего удалять)."""
    body = _extract_function(_db_source(), "delete_cached_analysis")
    assert 'redis_client.delete(f"llm_cache:' not in body, (
        "После удаления L1-записи и Redis-delete в этой функции не нужен"
    )
    assert "DELETE FROM llm_cache" in body


def test_cleanup_legacy_keys_function_exists_and_uses_unlink() -> None:
    """Функция миграции legacy-ключей существует и использует UNLINK
    (неблокирующее удаление) + SCAN батчами (не KEYS)."""
    src = _db_source()
    body = _extract_function(src, "cleanup_legacy_llm_cache_redis_keys")
    assert "scan_iter" in body, "Должна использоваться scan_iter, а не KEYS (KEYS блокирует Redis)"
    assert 'match="llm_cache:*"' in body, "Фильтр по prefix llm_cache:*"
    assert "unlink(" in body, "Удаление через UNLINK (неблокирующее, Redis 4.0+)"


def test_cleanup_legacy_keys_returns_zero_when_no_redis(monkeypatch) -> None:
    """Если Redis недоступен (get_redis_client вернул None) — функция
    возвращает 0 и не падает."""
    from app import db as db_mod

    monkeypatch.setattr(db_mod, "get_redis_client", lambda: None)
    assert db_mod.cleanup_legacy_llm_cache_redis_keys() == 0


def test_cleanup_legacy_keys_unlinks_in_batches(monkeypatch) -> None:
    """Проверяем что функция корректно проходит по SCAN и удаляет батчами."""
    from app import db as db_mod

    class FakeRedis:
        def __init__(self, keys):
            self.all_keys = list(keys)
            self.unlinked: list[list[str]] = []

        def scan_iter(self, match: str, count: int = 1000):
            # match должен быть точно "llm_cache:*"
            assert match == "llm_cache:*"
            for k in self.all_keys:
                yield k

        def unlink(self, *keys):
            self.unlinked.append(list(keys))
            return len(keys)

    sample_keys = [f"llm_cache:{i}" for i in range(2500)]
    fake = FakeRedis(sample_keys)
    monkeypatch.setattr(db_mod, "get_redis_client", lambda: fake)

    deleted = db_mod.cleanup_legacy_llm_cache_redis_keys(batch_size=1000)
    assert deleted == 2500
    # 2500 / 1000 = 2 полных батча + 1 финальный остаток (500).
    assert len(fake.unlinked) == 3
    assert sum(len(b) for b in fake.unlinked) == 2500


def test_worker_invokes_cleanup_on_startup() -> None:
    """Регрессия: worker.main вызывает cleanup_legacy_llm_cache_redis_keys
    при запуске в analysis-mode."""
    here = Path(__file__).resolve()
    worker_path = here.parent.parent.parent / "app" / "worker.py"
    src = worker_path.read_text(encoding="utf-8")
    assert "cleanup_legacy_llm_cache_redis_keys" in src, (
        "worker.main должен подключить миграцию для разовой чистки legacy-ключей"
    )
    # Должен быть импорт + вызов в run_analysis-ветке.
    assert "from app.db import (" in src
    # Достаточно проверить что вызов идёт ДО recovery_pass — чтобы Redis
    # освобождался до того как воркеры начнут гонять очередь.
    cleanup_pos = src.find("await asyncio.to_thread(cleanup_legacy_llm_cache_redis_keys)")
    recovery_pos = src.find("_recover_orphaned_reports_once)")
    assert cleanup_pos > 0 and recovery_pos > 0
    assert cleanup_pos < recovery_pos, (
        "Cleanup legacy-ключей должен идти ДО recovery-pass — чтобы освободить "
        "память Redis перед тем как воркеры начнут писать lease'ы и dedup-маркеры"
    )
