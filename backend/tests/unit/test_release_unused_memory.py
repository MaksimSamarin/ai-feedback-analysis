"""Юнит-тест на release_unused_memory (v2.1.2, BUG-23).

Регрессия: до v2.1.2 long-running воркеры на glibc накапливали фрагментацию
malloc — за неделю работы RSS рос с базовых ~100 МБ до 900+ МБ, упирался
в mem_limit и убивался OOM-killer'ом. Фикс — вызов release_unused_memory()
после каждой завершённой задачи (gc.collect + malloc_trim).

Запуск:
    cd backend && pytest tests/unit/test_release_unused_memory.py -v
"""
from __future__ import annotations

from pathlib import Path


def test_function_exists_and_callable() -> None:
    """Хелпер `release_unused_memory` импортируется из app.db и вызывается."""
    from app.db import release_unused_memory

    assert callable(release_unused_memory)
    result = release_unused_memory()
    # На glibc возвращает 0/1, на musl/при ошибке -1.
    assert isinstance(result, int)


def test_calls_gc_collect_and_malloc_trim(monkeypatch) -> None:
    """Внутри функции вызывается gc.collect() + libc.malloc_trim(0)."""
    import app.db as db_mod

    gc_called = {"n": 0}
    trim_calls: list[int] = []

    def fake_collect():
        gc_called["n"] += 1

    class FakeLibc:
        def malloc_trim(self, arg):
            trim_calls.append(arg)
            return 1  # «успешно отдали ОС какую-то память»

    monkeypatch.setattr(db_mod.gc, "collect", fake_collect)
    monkeypatch.setattr(db_mod, "_LIBC", FakeLibc())

    result = db_mod.release_unused_memory()

    assert gc_called["n"] == 1, "gc.collect() должен быть вызван"
    assert trim_calls == [0], "malloc_trim(0) — параметр 0 = «отдать всё что можешь»"
    assert result == 1


def test_returns_minus_one_when_libc_unavailable(monkeypatch) -> None:
    """На не-glibc платформах (musl/Alpine) возвращает -1, не падает."""
    import app.db as db_mod

    monkeypatch.setattr(db_mod, "_LIBC", None)

    result = db_mod.release_unused_memory()
    assert result == -1


def test_returns_minus_one_when_malloc_trim_raises(monkeypatch) -> None:
    """Если malloc_trim упал (редкое — некорректная сборка glibc), не валим
    main loop, возвращаем -1."""
    import app.db as db_mod

    class BrokenLibc:
        def malloc_trim(self, arg):
            raise OSError("libc broken")

    monkeypatch.setattr(db_mod, "_LIBC", BrokenLibc())

    result = db_mod.release_unused_memory()
    assert result == -1


def test_worker_calls_release_after_each_job() -> None:
    """Регрессия: worker.py main-loop вызывает release_unused_memory
    в finally после каждой задачи — для возврата фрагментации ОС."""
    here = Path(__file__).resolve()
    worker_src = (here.parent.parent.parent / "app" / "worker.py").read_text(encoding="utf-8")

    # Должен быть импорт.
    assert "release_unused_memory" in worker_src, (
        "worker.py должен импортировать release_unused_memory"
    )
    # И вызов через asyncio.to_thread (т.к. main loop async, а функция blocking).
    assert "asyncio.to_thread(release_unused_memory)" in worker_src, (
        "Вызов должен быть через to_thread, чтобы не блокировать event loop"
    )
    # Вызов должен быть в finally — чтобы срабатывал и на успех, и на ошибку.
    # Грубая проверка: finally-блок где-то перед вызовом.
    call_pos = worker_src.find("asyncio.to_thread(release_unused_memory)")
    finally_pos = worker_src.rfind("finally:", 0, call_pos)
    assert finally_pos > 0, (
        "release_unused_memory должен вызываться в finally — иначе на исключении пропустится"
    )
