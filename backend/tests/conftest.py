"""Общие pytest-фикстуры для юнит-тестов.

Путь к `backend/` добавляется в sys.path, чтобы работал `import app....` без
установки пакета.
"""

from __future__ import annotations

import sys
from pathlib import Path

_BACKEND_DIR = Path(__file__).resolve().parent.parent
if str(_BACKEND_DIR) not in sys.path:
    sys.path.insert(0, str(_BACKEND_DIR))
