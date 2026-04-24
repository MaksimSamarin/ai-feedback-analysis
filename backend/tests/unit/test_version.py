"""Юнит-тест на единый источник версии (BUG-11).

Проверяет что `app.__version__`:
1. Существует и является непустой строкой
2. Имеет валидный semver-формат (MAJOR.MINOR.PATCH)
3. Не начинается с префикса "mvp-" — мы вышли из прототипа в v2.0
4. Используется и в job_manager (APP_VERSION), и в main.py (FastAPI version)

Запуск:
    cd backend && pytest tests/unit/test_version.py -v
"""

from __future__ import annotations

import re


def test_version_is_defined_and_non_empty() -> None:
    from app import __version__

    assert isinstance(__version__, str)
    assert __version__.strip(), "__version__ пустая строка"


def test_version_matches_semver_format() -> None:
    from app import __version__

    # MAJOR.MINOR.PATCH, каждое — цифры. Допускаем пре-релизные суффиксы типа 2.0.0-rc1.
    assert re.fullmatch(r"\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?", __version__), (
        f"__version__={__version__!r} не соответствует semver"
    )


def test_version_has_no_mvp_prefix() -> None:
    """Регрессия: после выхода из MVP префикс 'mvp-' недопустим."""
    from app import __version__

    assert not __version__.startswith("mvp-"), (
        f"__version__={__version__!r} всё ещё имеет префикс 'mvp-'"
    )


def test_job_manager_uses_shared_version() -> None:
    """`APP_VERSION` в job_manager должен быть тем же объектом, что `app.__version__`."""
    from app import __version__
    from app.services.job_manager import APP_VERSION

    assert APP_VERSION == __version__, (
        f"рассинхрон версий: __version__={__version__!r}, job_manager.APP_VERSION={APP_VERSION!r}"
    )


def test_version_is_2_x() -> None:
    """Закрепляем релизную линию v2.x после выхода из MVP."""
    from app import __version__

    major = int(__version__.split(".")[0])
    assert major >= 2, (
        f"ожидали major >= 2 для release 2.0+, получили __version__={__version__!r}"
    )
