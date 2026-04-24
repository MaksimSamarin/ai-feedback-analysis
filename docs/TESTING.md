# Тесты: как запускать и что проверяется

Живой документ, отражающий **текущее состояние** тестов в проекте.

---

## TL;DR

```bash
cd backend
pip install -r requirements-dev.txt
python -m pytest tests/unit -v
```

Ожидание: **245 passed** примерно за 12 секунд.

---

## Установка зависимостей для тестов

Рекомендуемый способ — через `requirements-dev.txt`:

```bash
cd backend
pip install -r requirements-dev.txt
```

Там зафиксированы версии:

| Пакет | Зачем |
|-------|-------|
| `pytest` | Запуск и отчёт |
| `pytest-asyncio` | Async-тесты (`Job.emit`, cleanup logic) |
| `psutil` | Замер RSS процесса в тесте на большие отчёты |

На будущее (когда добавим сценарные/интеграционные тесты):
`pytest-cov`, `pytest-timeout`, `fakeredis`, `testcontainers[postgres]`, `httpx`, `numpy`, `openpyxl`.

---

## Как запускать

Все команды — из директории `backend/`.

### Все юнит-тесты

```bash
python -m pytest tests/unit -v
```

### Один файл

```bash
python -m pytest tests/unit/test_export_streaming.py -v
```

### Один конкретный тест

```bash
python -m pytest tests/unit/test_version.py::test_version_is_2_x -v
```

### Только медленные (нагрузочные)

```bash
python -m pytest -m slow -v
```

### Пропустить медленные

```bash
python -m pytest -m "not slow" -v
```

### С отчётом о покрытии (когда поставим `pytest-cov`)

```bash
pip install pytest-cov
python -m pytest tests/unit --cov=app --cov-report=html
# Открыть htmlcov/index.html в браузере
```

---

## Структура

```
backend/
├── pytest.ini                    # конфигурация pytest + маркеры + asyncio_mode=auto
├── requirements-dev.txt          # pytest, pytest-asyncio, psutil (фикс. версии)
├── tests/
│   ├── __init__.py
│   ├── conftest.py               # фикстуры: добавляет backend/ в sys.path
│   └── unit/
│       ├── __init__.py
│       ├── test_context_length_error.py         # BUG-05 — лимит контекста LLM
│       ├── test_export_streaming.py             # BUG-14 — стриминг-экспорт
│       ├── test_get_conn_transactions.py        # BUG-08 — rollback в get_conn()
│       ├── test_job_cleanup_after_finish.py     # BUG-01 — отложенная очистка jobs
│       ├── test_auth.py                         # U5 — Argon2 + PBKDF2 совместимость
│       ├── test_build_report_analysis.py        # Итерация 1 — упрощённый /analysis с preview_rows
│       ├── test_build_summary_from_db.py        # U8 — SQL-агрегация summary (упрощена в итерации 1)
│       ├── test_compute_cache_key.py            # U7 — генерация ключа кэша LLM
│       ├── test_crypto.py                       # U4 — шифрование API-ключей (Fernet)
│       ├── test_excel_normalize_review.py       # U1 — парсинг ячейки Excel, регрессия BUG-05
│       ├── test_excel_numeric_cells.py          # Итерация 1 — int/float/bool сохраняются как Number/Boolean
│       ├── test_group_key_index.py              # BUG-09 — partial-индекс на group_key
│       ├── test_iter_report_rows.py             # BUG-14 — батчевая пагинация строк отчёта
│       ├── test_normalize_api_key.py            # BUG-07 — очистка OpenAI токена + регрессия дубля
│       ├── test_queue_redis_shared.py           # BUG-04 — единый Redis-пул
│       ├── test_redis_emit_shared_pool.py       # BUG-03 — emit через shared pool
│       ├── test_render_prompt_retry_feedback.py # Итерация 1.2 — обратная связь модели при retry
│       ├── test_render_prompt_row_json_fallback.py # Итерация 3.2 — fallback {row_json} под капот, удалён {row_number}, формат инструкции про enum
│       ├── test_inspect_xlsx_unique_counts.py   # Итерация 3.2 — расчёт unique_counts в inspect для показа «будет N групп»
│       ├── test_cache_invalidation.py           # Итерация 3.2 — инвалидация битого cache-hit + пропуск кэша при retry-feedback
│       ├── test_retry_reset_rows.py             # Итерация 3.2 — retry сбрасывает error/skipped строки + preview по группам
│       ├── test_export_group_dedup.py           # Итерация 3.2 — xlsx-выгрузка группового отчёта дедуплицирует по group_key
│       ├── test_reports_cleanup_respects_config.py  # BUG-02 — enforce_reports_limit удалена
│       ├── test_row_to_preview.py               # Итерация 1 — превью-DTO с динамическими колонками
│       ├── test_schema_shapes.py                # Итерация 1 — форма Pydantic-моделей после упрощения
│       ├── test_schemas_validation.py           # U6 — Pydantic валидаторы API-контракта
│       ├── test_to_db_query.py                  # BUG-10 — regex-замена плейсхолдеров
│       ├── test_validate_custom_output_no_core.py  # Итерация 1 — валидатор без core_required_fields
│       └── test_version.py                      # BUG-11 — единая версия приложения
```

### Маркеры (из `pytest.ini`)

| Маркер | Что значит |
|--------|------------|
| `slow` | Тесты длительностью > 5 секунд — нагрузочные (генерация 200k строк, замер RSS) |
| `scenario` | Интеграционные с testcontainers (Docker). Пока не реализовано. |
| `real_llm` | Прогон с реальным LLM API (до 100 строк). Пока не реализовано. |

---

## Текущий набор тестов (225 штук)

### IDEA-07 — позиция в очереди

- `test_queue_position.py` (9 тестов) — `get_job_queue_position` / `get_inspect_queue_position`:
  - Первая задача — позиция 0; middle — позиция >0; последняя — корректно; отсутствует — None.
  - Пустая очередь — None; пустой id — None без обращения к Redis (fast-path).
  - Мусор среди payload'ов (не JSON / не dict) не роняет поиск, индекс считается по общему списку.
  - Inspect-очередь матчится по `file_id`, analysis — по `job_id`.
  - Redis падает (`lrange` бросает) — функция возвращает None, не пробрасывает исключение.
- `test_queue_remove.py` (7 тестов) — `remove_job_from_queue` / `remove_inspect_from_queue`:
  - LREM по payload в середине списка возвращает 1.
  - Задача не в очереди (взята воркером) — LREM не вызывается, dedup-маркер всё равно чистится.
  - Пустой id — ранний выход без Redis-вызовов.
  - Дубли payload'а — LREM вызывается по каждому вхождению.
  - Мусорный элемент (не JSON) пропускается, таргет дальше по списку находится.
  - Inspect-версия бьёт в INSPECT_QUEUE_KEY по `file_id`.
  - Падение `lrange` не пробрасывает исключение — функция возвращает 0.

### Итерация 2 — новые контракты

- `test_sanitize_download_filename.py` (14 тестов) — безопасное имя файла: None/пустая → fallback, slash/backslash/control-chars заменяются на `_`, пользовательское расширение срезается, кириллица сохраняется, длина stem ограничена 100 символами, защита от path traversal.
- `test_partial_export_from_db.py` (2 теста) — `export_results_xlsx` на смеси готовых/упавших/pending строк: все строки попадают в xlsx, колонки результата пустые у pending, error-текст на месте у упавших, флаг `partial` в summary-листе. Плюс fallback без явного status.
- `test_retry_forces_use_cache.py` (5 тестов) — контракт retry: payload из `build_job_payload_from_report` корректно читает `use_cache` из БД (True/False), override на `use_cache=True` не затрагивает остальные поля (provider/model/prompt/parallelism/temperature/analysis_columns).

### BUG-15 — устойчивость воркера к рестарту Postgres

- `test_worker_db_recovery.py` (9 тестов):
  - `reset_pg_pool` — noop без пула, закрытие+обнуление глобальной ссылки, глушение ошибок `close()` у битого пула.
  - `requeue_after_transient_error` — file_inspect попадает в INSPECT_QUEUE_KEY, analysis в QUEUE_KEY, обход dedup-маркера (ключевой контракт), возврат False при Redis-ошибке.
  - Main-loop воркера — восстанавливается и requeue-ит payload после `psycopg.OperationalError` для обоих типов задач; non-transient ошибка по-прежнему помечает report `failed`.

Все тесты — чистые юниты: не требуют Postgres, Redis, Docker, сети, LLM.
Внешние зависимости мокаются через `monkeypatch.setattr` или `MagicMock`.
Async-тесты (emit, cleanup) запускаются через `pytest-asyncio` в `asyncio_mode=auto`.

### `test_context_length_error.py` — 7 тестов, покрывают **BUG-05** (лимиты длины промпта)

| Тест | Что проверяет |
|------|---------------|
| `test_looks_like_context_exceeded_matches_error_codes` | Детектор срабатывает на `context_length_exceeded`, `string_above_max_length` (в т.ч. в верхнем регистре) |
| `test_looks_like_context_exceeded_matches_message_hints` | Срабатывает по тексту: "maximum context length", "too many tokens", "too long" |
| `test_looks_like_context_exceeded_rejects_unrelated_errors` | `invalid_api_key`, `rate_limit_exceeded`, пустые — не классифицируются |
| `test_removed_constants_not_in_config` | Регрессия: `MAX_REVIEW_CHARS`, `LLM_MAX_PROMPT_CHARS`, `LLM_SKIP_OVERSIZED_REQUESTS` отсутствуют в `app.config` |
| `test_context_length_exceeded_carries_model_and_message` | Исключение хранит `model` и `provider_message` |
| `test_context_length_exceeded_is_plain_exception` | Наследуется от `Exception`, не от HTTP-класса |
| `test_context_length_exceeded_default_message` | При пустом `provider_message` в `str(exc)` есть имя модели и слово "context" |

### `test_export_streaming.py` — 5 тестов, покрывают **BUG-14** (OOM на экспорте больших отчётов) и итерацию 1.3 (чистый итоговый xlsx)

| Тест | Что проверяет |
|------|---------------|
| `test_export_results_xlsx_streams_without_materializing_rows` | На 5000 строк фабрика итератора вызывается ровно 2 раза (discovery + write), все строки доходят до файла, заголовки собраны из passthrough/analysis JSON; ключи из `input_json` в итог НЕ попадают |
| `test_export_results_xlsx_uses_write_only_mode` | Workbook создаётся с `write_only=True` — регрессия на ключевое решение фикса |
| `test_export_results_xlsx_excludes_analysis_input_columns` | Исходные колонки анализа в итоговом xlsx не пишутся отдельными столбцами (они уже в `review_text`); справочные колонки и результат модели — на месте |
| `test_export_results_xlsx_allows_column_both_in_analysis_and_passthrough` | Если одна колонка выбрана и для анализа, и в итоговый отчёт — в xlsx она один раз (через passthrough), дубля нет |
| `test_export_results_xlsx_memory_stays_bounded_on_large_report` (`@slow`) | На 200 000 строк прирост RSS < 200 МБ — признак стриминга (без фикса было бы 500+ МБ) |

### `test_get_conn_transactions.py` — 5 тестов, покрывают **BUG-08** (explicit rollback)

| Тест | Что проверяет |
|------|---------------|
| `test_get_conn_commits_on_success` | Happy path: вызывается `commit()`, не вызывается `rollback()` |
| `test_get_conn_rolls_back_on_exception` | При исключении внутри `with` вызывается `rollback()`, исключение пробрасывается |
| `test_get_conn_commits_called_exactly_once` | Защита от двойного коммита (например, если кто-то добавит `commit` и в try, и в finally) |
| `test_get_conn_rollback_failure_does_not_hide_original_exception` | Если сам `rollback()` упал — оригинальное исключение пользователя не подменяется шумом от ошибки отката |
| `test_get_conn_commit_failure_triggers_rollback` | Если упал сам `commit()` (сеть, deadlock) — `rollback()` всё равно вызывается |

### `test_job_cleanup_after_finish.py` — 7 тестов, покрывают **BUG-01** (утечка памяти в `JobManager.jobs`)

| Тест | Что проверяет |
|------|---------------|
| `test_delayed_cleanup_removes_job_from_dict` | `_delayed_cleanup(delay=0)` удаляет запись из `self.jobs` |
| `test_delayed_cleanup_is_idempotent` | Повторный вызов на отсутствующий ключ не падает |
| `test_delayed_cleanup_handles_cancelled_error` | При `CancelledError` во время `asyncio.sleep` память всё равно освобождается |
| `test_run_job_with_cleanup_schedules_cleanup_on_success` | При успехе `_run_job` cleanup планируется через `asyncio.create_task` |
| `test_run_job_with_cleanup_schedules_cleanup_on_failure` | При падении `_run_job` cleanup планируется через `try/finally`, исключение пробрасывается |
| `test_cleanup_delay_reads_from_env` | `JOB_CLEANUP_DELAY_SEC=42` → при `importlib.reload` константа читается |
| `test_cleanup_delay_has_sane_default` | Дефолт — ровно 300 секунд (5 минут), регрессия на согласованное значение |

### `test_queue_redis_shared.py` — 2 теста, покрывают **BUG-04** (единый Redis-пул)

| Тест | Что проверяет |
|------|---------------|
| `test_queue_redis_shares_instance_with_db` | `queue._redis()` и `db.get_redis_client()` возвращают **тождественный** (`is`) объект — пулы не разделились |
| `test_queue_redis_raises_when_client_unavailable` | При `get_redis_client() is None` поднимается `RuntimeError` с сообщением "Redis недоступен" |

### `test_redis_emit_shared_pool.py` — 6 тестов, покрывают **BUG-03** (TCP-соединение на каждый emit)

| Тест | Что проверяет |
|------|---------------|
| `test_emit_reuses_shared_client` | 100 emit-ов → одинаково `get_redis_client` вызван, клиент переиспользуется |
| `test_emit_never_calls_redis_from_url` | `Redis.from_url` не вызывается при 10 emit-ах — регрессия на старое поведение |
| `test_emit_skips_if_client_is_none` | При `get_redis_client() → None` emit не падает, событие в локальной `event_queue` |
| `test_emit_swallows_publish_errors` | `publish` кидает `ConnectionError` → emit не поднимает exception |
| `test_emit_uses_to_thread_for_publish` | Публикация идёт через `asyncio.to_thread` — event loop не блокируется |
| `test_job_manager_source_has_no_redis_from_url` | Регрессия на уровне исходников: `Redis.from_url` не появляется в `job_manager.py` |

### `test_reports_cleanup_respects_config.py` — 2 теста, покрывают **BUG-02** (хардкод `20` в очистке)

| Тест | Что проверяет |
|------|---------------|
| `test_enforce_reports_limit_removed` | Функция `enforce_reports_limit` удалена из `db.py`, `from app.db import enforce_reports_limit` → `ImportError` |
| `test_job_manager_source_has_no_enforce_calls` | Регрессия на уровне исходников: `enforce_reports_limit` не упоминается в `job_manager.py` |

### `test_auth.py` — 11 тестов, покрывают **U5** (пароли: Argon2 + PBKDF2 legacy)

| Тест | Что проверяет |
|------|---------------|
| `test_argon2_hash_verify_roundtrip` | Happy path: hash → verify = True, хэш начинается с `$argon2` |
| `test_verify_rejects_wrong_password` | Неверный пароль / пустой / с лишним пробелом → False |
| `test_hash_does_not_contain_plaintext` | Plaintext пароль не просачивается в хэш |
| `test_hash_produces_different_hashes_each_time` | Одинаковые пароли → разные хэши (salt рандомный) |
| `test_legacy_pbkdf2_hash_still_verifies` | Старый формат `salt_hex:digest_hex` продолжает верифицироваться (обратная совместимость) |
| `test_password_needs_rehash_true_for_legacy_pbkdf2` | PBKDF2 хэш → нужен rehash в Argon2 |
| `test_password_needs_rehash_false_for_fresh_argon2` | Свежий Argon2-хэш — rehash не нужен |
| `test_validate_password_policy_accepts_valid` | Валидные пароли (латиница/кириллица + цифра + 8+ символов) — `None` |
| `test_validate_password_policy_rejects_short_password` | Меньше 8 символов — ошибка с упоминанием длины |
| `test_validate_password_policy_rejects_no_digit` | Без цифр — ошибка про "цифру" |
| `test_validate_password_policy_rejects_no_letter` | Без букв — ошибка про "букву" |

### `test_crypto.py` — 7 тестов, покрывают **U4** (шифрование API-ключей через Fernet)

| Тест | Что проверяет |
|------|---------------|
| `test_encrypt_decrypt_roundtrip` | Happy path: encrypt → decrypt = исходная строка, префикс `v2:` |
| `test_encrypt_produces_different_ciphertext_each_time` | Двойной encrypt одного текста даёт **разные** шифротексты (рандомный IV) |
| `test_decrypt_with_wrong_key_raises_invalid_token` | Смена `APP_SECRET` → старый шифротекст кидает `InvalidToken`, а не мусор |
| `test_encrypt_empty_string_roundtrip` | Пустая строка — валидный вход, roundtrip работает |
| `test_encrypt_unicode_roundtrip` | Кириллица + эмоджи + японский — сохраняются без потерь |
| `test_decrypt_rejects_unversioned_token` | Шифротекст без префикса `v2:` — `InvalidToken` (версионирование) |
| `test_encrypt_without_app_secret_raises` | Без `APP_SECRET` в env — `RuntimeError` с упоминанием переменной |

### `test_excel_normalize_review.py` — 20 тестов, покрывают **U1** (нормализация ячейки Excel)

Параметризованные тесты на маркеры пустоты — `nan`/`null`/`n/a`/`na`/`-` в любом регистре.

| Тест | Что проверяет |
|------|---------------|
| `test_none_returns_none_with_warning` | `None` → `(None, ["empty_cell"])` |
| `test_empty_string_returns_none_with_warning` | `""` → empty_cell |
| `test_whitespace_only_returns_none` | `"   "`, `"\t\n"`, `"\r\n  "` → empty_cell |
| `test_empty_markers_return_none` | Параметризовано: `nan`, `none`, `null`, `n/a`, `na`, `-` → empty_cell |
| `test_empty_markers_case_insensitive` | Параметризовано: `NaN`, `NULL`, `N/A`, `None`, `NA` → empty_cell |
| `test_valid_text_returned_as_is` | Обычный отзыв возвращается без изменений, warnings пустой |
| `test_strips_surrounding_whitespace` | Внешние пробелы/таб/`\n` убираются |
| `test_long_text_not_truncated` | **Регрессия BUG-05**: 50 000 символов возвращаются целиком |
| `test_number_converted_to_string` | `42` и `3.14` → `"42"` и `"3.14"` |
| `test_zero_is_valid_not_empty` | `0` — валидное значение (рейтинг/счётчик), не empty_cell |
| `test_unicode_and_emoji_preserved` | Кириллица + эмоджи + японский — без потерь |

### `test_build_summary_from_db.py` — 7 тестов, покрывают **U8** (SQL-агрегация summary)

После итерации 1 (отказ от обязательных полей) функция упрощена: возвращает только total/processed/success/failed. Sentiment-агрегаты выпилены.

| Тест | Что проверяет |
|------|---------------|
| `test_empty_report_returns_zero_fields` | Пустой отчёт — все 4 агрегата 0, без падений |
| `test_all_failed_report` | 10 строк все failed — success_rows=0, failed_rows=10 |
| `test_mixed_success_failed` | total=100, success=95, failed=5 — агрегаты корректные |
| `test_empty_agg_response_handled` | `get_report_summary_agg` вернул `{}` — не KeyError, дефолтные нули |
| `test_result_compatible_with_job_summary_pydantic_model` | Результат распаковывается в `JobSummary(**payload)` без ошибок |
| `test_processed_rows_equals_total_rows` | `processed_rows == total_rows` (финальная агрегация) |
| `test_no_sentiment_fields_in_summary` | Регрессия: `avg_negativity_score`, `sentiment_counts`, `sentiment_percentages` НЕ возвращаются |

### `test_build_report_analysis.py` — 5 тестов, покрывают итерацию 1 (упрощённый `/analysis`)

Сборщик ответа на `/api/reports/{id}/analysis`. Возвращает простую сводку + preview_rows.

| Тест | Что проверяет |
|------|---------------|
| `test_summary_has_four_technical_fields_only` | Сводка содержит ровно total/processed/success/failed |
| `test_no_sentiment_fields_in_summary` | Регрессия: sentiment_counts/avg_negativity_score/sentiment_percentages не возвращаются |
| `test_preview_limit_respected` | Превью не больше `preview_limit` (по умолчанию 10), даже при 20 строках |
| `test_preview_handles_empty_report` | Пустой отчёт — пустой preview, без ошибки |
| `test_preview_contains_dynamic_columns_from_arbitrary_schema` | Произвольная схема без core-полей — превью строится из того что LLM вернул |

### `test_row_to_preview.py` — 11 тестов, покрывают итерацию 1 (превью-DTO)

`_row_to_preview(row)` превращает строку `report_rows` в DTO с динамическими колонками.

| Тест | Что проверяет |
|------|---------------|
| `test_basic_merge_input_passthrough_custom` | Три источника колонок (input/passthrough/custom) сливаются в плоский dict |
| `test_custom_json_overrides_passthrough` | При конфликте ключа побеждает custom (анализ главнее) |
| `test_nested_dict_in_custom_is_flattened` | Вложенные dict разворачиваются в `"parent.child"` |
| `test_numeric_and_bool_types_preserved` | `int`/`float`/`bool` в columns сохраняются как есть, не str |
| `test_invalid_json_in_custom_returns_empty_columns` | Кривой JSON → пустые columns, без падений |
| `test_dict_passed_as_is_without_serialization` | Если поле уже dict (не JSON-строка) — работает |
| `test_warnings_list_preserved` | Список warnings сохраняется |
| `test_warnings_json_string_decoded` | Warnings в JSON-строке декодируются в список |
| `test_error_text_maps_to_error_field` | `error_text` → поле `error` DTO |
| `test_empty_error_becomes_none` | Пустая строка/None → `error=None` |
| `test_no_core_fields_imposed` | Регрессия: не требует summary/category/confidence |

### `test_render_prompt_retry_feedback.py` — 7 тестов, покрывают итерацию 1.2 (обратная связь модели при retry)

`_render_prompt` принимает `retry_feedback={"error": ..., "previous_response": ...}` и добавляет в конец промпта блок с причиной ошибки предыдущей попытки и её невалидным ответом (≤500 символов). Модель видит в чём ошиблась и шанс исправления на 2-й попытке вырастает до 60-80%.

| Тест | Что проверяет |
|------|---------------|
| `test_no_feedback_on_first_attempt` | Без `retry_feedback` промпт не содержит блок — первая попытка чистая |
| `test_feedback_block_appended_when_provided` | С `retry_feedback` добавляется блок "Предыдущий ответ не прошёл проверку" + текст ошибки + прошлый ответ |
| `test_previous_response_dict_serialized_as_json` | Dict → JSON с `ensure_ascii=False` (кириллица не экранируется) |
| `test_previous_response_truncated_to_500_chars` | Длинный ответ обрезается до 500 символов + многоточие |
| `test_feedback_after_expected_json_block` | EXPECTED_JSON идёт выше feedback-блока (порядок: схема → ошибка относительно схемы) |
| `test_feedback_without_previous_response_only_has_error` | `previous_response=None` → в промпте только причина, без блока "Прошлый ответ" |
| `test_feedback_mentions_what_model_should_do` | В feedback есть явная инструкция "исправь и верни по EXPECTED_JSON без повторения ошибок" |

### `test_export_group_dedup.py` — 4 теста, покрывают итерацию 3.2 (xlsx-выгрузка группового отчёта — одна строка на группу)

В групповом режиме LLM возвращает один ответ на группу, применяемый ко всем строкам группы. Раньше в xlsx попадали все строки группы с дублирующимся analysis (группа из 20 строк = 20 одинаковых записей). Теперь `export_results_xlsx` дедуплицирует по `group_key` (как и `build_report_analysis` для preview).

| Тест | Что проверяет |
|------|---------------|
| `test_grouped_export_emits_one_row_per_group` | 6 строк в 3 группах → в xlsx ровно 3 записи |
| `test_non_grouped_export_keeps_all_materialized_rows` | group_key=None — все done-строки попадают в xlsx (регрессия) |
| `test_grouped_export_skips_pending_rows` | Pending-строки не попадают даже в групповой выгрузке |
| `test_grouped_export_empty_group_key_treated_as_ungrouped` | group_key=`""` не дедуплицирует — ведёт себя как негрупповая запись |

### `test_retry_reset_rows.py` — 4 теста, покрывают итерацию 3.2 (Перезапуск сбрасывает упавшие/скипнутые строки + превью по группам)

При Retry сервис вызывает `reset_failed_and_skipped_rows(report_id)` — возвращает в pending все строки со status='error' и строки со status='done' с warning `skipped_large_group` (скипнутые по старому лимиту группы). Плюс `build_report_analysis` для групповых отчётов возвращает одну строку на group_key в превью.

| Тест | Что проверяет |
|------|---------------|
| `test_reset_failed_and_skipped_rows_function_exists` | Функция существует в db-слое |
| `test_reset_sql_targets_error_and_skipped` | SQL: `UPDATE report_rows SET status='pending', ... WHERE (status='error' OR skipped_large_group)`, чистятся custom_json/warnings/error_text |
| `test_build_report_analysis_groups_preview_by_group_key` | Для группового отчёта в preview — одна строка на group_key |
| `test_build_report_analysis_non_grouped_keeps_row_level` | Для обычного отчёта preview построчный (регрессия) |

### `test_cache_invalidation.py` — 4 теста, покрывают итерацию 3.2 (инвалидация битого кэша, skip при retry-feedback)

Если в кэше лежит запись, которая уже не проходит валидацию (например, схема поменялась), сервис при `cache_hit` ловит `ValidationError`, **удаляет** запись из кэша (Redis + БД) и идёт к модели реальным запросом. Параллельно: при `retry_feedback` (модель возвращает невалидный JSON, сервис отдаёт ей обратную связь) кэш на повторной попытке **пропускается** — иначе тот же битый ответ обнулит retry-эффект. В `warnings` для строки пишется `cache_invalidated:<причина>` — видно в xlsx-выгрузке.

| Тест | Что проверяет |
|------|---------------|
| `test_delete_cached_analysis_function_exists` | В db-слое есть функция `delete_cached_analysis(cache_key)` |
| `test_process_row_skips_cache_on_retry_feedback` | В `_process_row` условие `if use_cache and not retry_feedback:` — при retry с feedback кэш не читается |
| `test_invalid_cache_hit_triggers_delete` | Битый cache-hit ведёт к `delete_cached_analysis(cache_key)` + warning `cache_invalidated` |
| `test_delete_cached_analysis_removes_redis_and_db` | `delete_cached_analysis` выполняет `redis.delete('llm_cache:<key>')` и `DELETE FROM llm_cache WHERE cache_key = ?` |

### `test_inspect_xlsx_unique_counts.py` — 7 тестов, покрывают итерацию 3.2 (расчёт уникальных значений при inspect)

`inspect_xlsx` одновременно с `total_rows` считает `unique_counts: dict[col, int|None]` — количество уникальных значений в каждой колонке. Используется UI для показа «будет обработано K групп» при выборе колонки группировки. Защищено `INSPECT_UNIQUE_CAP` (default 100_000, ENV override) от OOM на колонках типа review_text/id: при превышении `unique_counts[col] = None`.

| Тест | Что проверяет |
|------|---------------|
| `test_unique_counts_present_in_result` | Поле `unique_counts` всегда присутствует, ключи = имена колонок |
| `test_unique_counts_values_correct` | Подсчёт корректен на известном наборе (операторы, категории) |
| `test_unique_counts_ignores_empty_cells` | Пустые ячейки, пробельные строки, None не учитываются |
| `test_unique_counts_cap_returns_none_when_exceeded` | При превышении cap — `None` (защита от OOM) |
| `test_unique_counts_cap_allows_exactly_cap_count` | Ровно cap — ещё считается, возвращается число |
| `test_unique_counts_handles_mixed_types` | Числа, строки, float, unicode считаются без ошибок |
| `test_total_rows_still_computed_correctly` | Регрессия: `total_rows` не сломан после добавления unique-подсчёта |

### `test_render_prompt_row_json_fallback.py` — 7 тестов, покрывают итерацию 3.2 (скрытие `{row_json}` под капот)

`_render_prompt` работает без обязательной переменной `{row_json}` в пользовательском шаблоне: если её нет — сервис автоматически дописывает блок `Данные строки:\n<input_json>` перед EXPECTED_JSON. Параллельно удалён мёртвый код подстановки `{row_number}` и переписана инструкция про enum-поля под реальный формат схемы (`type: enum` + массив `values`).

| Тест | Что проверяет |
|------|---------------|
| `test_row_json_placeholder_replaced_when_present` | Если в шаблоне есть `{row_json}` — заменяется на `input_json` |
| `test_row_json_appended_as_fallback_when_missing` | Если переменной нет — блок `Данные строки:` дописывается автоматически |
| `test_row_json_fallback_uses_empty_object_when_input_empty` | При пустом `input_json` fallback подставляет `{}` |
| `test_row_number_placeholder_not_substituted` | `{row_number}` больше не подставляется (мёртвый код удалён) |
| `test_render_prompt_signature_has_no_row_number` | Регрессия: `row_number` удалён из сигнатуры `_render_prompt` |
| `test_enum_instruction_matches_expected_json_format` | Инструкция к LLM про enum использует формулировку `type: enum` + `values` (а не ключ `enum`, которого в EXPECTED_JSON нет) |
| `test_fallback_preserves_expected_json_block_order` | Данные строки идут ДО EXPECTED_JSON (данные → формат ответа) |

### `test_validate_custom_output_no_core.py` — 6 тестов, покрывают итерацию 1 (валидатор без core-полей)

`_validate_custom_output` больше не требует `summary/category/confidence`. Работает только по пользовательской схеме.

| Тест | Что проверяет |
|------|---------------|
| `test_arbitrary_schema_without_core_fields_passes` | Схема без core — ответ проходит |
| `test_missing_summary_category_confidence_not_rejected` | Хардкод-проверки нет; схема с summary но без summary в ответе — не падает |
| `test_output_schema_required_still_enforced` | Пользовательский `required` продолжает работать |
| `test_extra_keys_dropped_and_warned` | Лишние ключи отбрасываются и попадают в warnings |
| `test_non_dict_response_rejected` | Non-dict ответ — явная ошибка |
| `test_source_has_no_core_required_fields_hardcode` | Source-grep регрессия: `core_required_fields` нет в коде |

### `test_schema_shapes.py` — 9 тестов, покрывают итерацию 1 (форма Pydantic-моделей)

Регрессия на схемы API после упрощения: `JobSummary`, `PreviewRow`, `ReportAnalysisResponse`, `JobResult`.

| Тест | Что проверяет |
|------|---------------|
| `test_job_summary_has_only_four_technical_fields` | JobSummary содержит ровно total/processed/success/failed |
| `test_job_summary_rejects_missing_required_fields` | Неполный dict — Pydantic падает |
| `test_job_summary_accepts_valid_payload` | Базовая сборка модели |
| `test_preview_row_exists_and_has_expected_fields` | PreviewRow имеет row_number/columns/warnings/error |
| `test_preview_row_columns_is_arbitrary_dict` | `columns` принимает русские ключи, числа, boolean |
| `test_preview_row_empty_columns_default` | Пустой row — columns `{}`, warnings `[]`, error `null` |
| `test_report_analysis_response_uses_preview_rows_not_top_negative` | У ReportAnalysisResponse есть `preview_rows`, нет `top_negative` |
| `test_top_negative_item_removed_from_schemas` | `TopNegativeItem` удалена из `schemas.py` |
| `test_job_result_uses_preview_rows` | JobResult перешёл на preview_rows |

### `test_excel_numeric_cells.py` — 5 тестов, покрывают итерацию 1 (числа в xlsx)

Регрессия: `int`/`float`/`bool` из `analysis_json` попадают в xlsx-ячейки как Number/Boolean, не String. Проверяем записью в BytesIO и чтением обратно через openpyxl.

| Тест | Что проверяет |
|------|---------------|
| `test_int_stays_number_in_xlsx` | `9` в custom_json → ячейка `int`, не `"9"` |
| `test_float_stays_number_in_xlsx` | `4.75` → `float`, знак после запятой не теряется |
| `test_bool_stays_bool_in_xlsx` | `True` → `TRUE`, openpyxl отдаёт `bool` |
| `test_int_in_passthrough_stays_number` | Числа в passthrough (из исходного Excel) тоже остаются числами |
| `test_mixed_numeric_types_in_same_row` | Смесь str/int/float/bool в одной строке — каждый тип сохранён |

### `test_group_key_index.py` — 5 тестов, покрывают **BUG-09** (partial-индекс на group_key)

Source-grep регрессия. Живая БД не нужна — проверяем что в `init_db` есть корректный `CREATE INDEX`.

| Тест | Что проверяет |
|------|---------------|
| `test_group_key_index_is_created` | Имя `idx_report_rows_group_key` присутствует в `db.py` |
| `test_group_key_index_uses_correct_columns` | Индекс на `(report_id, group_key, status)` — порядок важен |
| `test_group_key_index_is_partial` | Есть `WHERE group_key IS NOT NULL` — экономия места для не-grouped отчётов |
| `test_group_key_index_uses_if_not_exists` | `CREATE INDEX IF NOT EXISTS` — повторный старт не падает |
| `test_index_created_on_parent_partitioned_table` | Создаётся на родителя `report_rows`, не на партицию (`report_rows_p0` и т.п.) |

### `test_iter_report_rows.py` — 9 тестов, покрывают **BUG-14** (батчевая пагинация строк)

| Тест | Что проверяет |
|------|---------------|
| `test_empty_report_yields_nothing` | Пустой отчёт — 0 yield, ровно 1 SQL (не бесконечный цикл) |
| `test_single_batch_when_rows_fewer_than_limit` | 100 строк при batch_size=2000 — 1 запрос, все строки |
| `test_multiple_batches_when_rows_exceed_limit` | 5000 строк при batch_size=2000 — 3 запроса, порядок сохранён |
| `test_offsets_increase_across_batches` | OFFSETы идут 0, batch_size, 2×batch_size — пагинация корректна |
| `test_exact_multiple_of_batch_size` | 4000 при batch_size=2000 — 2 полных + 1 пустой пинг-запрос |
| `test_yields_rows_as_dicts` | Каждый yield — `dict`, не Row-объект psycopg (у downstream есть `.get()`) |
| `test_sql_uses_order_by_row_number` | Регрессия: `ORDER BY row_number` в SQL — иначе два прохода дают разный порядок |
| `test_report_id_passed_as_first_param` | Параметры `(report_id, limit, offset)` в правильном порядке |
| `test_custom_batch_size_respected` | Кастомный `batch_size` попадает в `LIMIT` |

### `test_normalize_api_key.py` — 12 тестов, покрывают **BUG-07** (дубль функции убран)

| Тест | Что проверяет |
|------|---------------|
| `test_returns_empty_for_none` | `None` → `""` |
| `test_returns_empty_for_empty_string` | `""` → `""` |
| `test_returns_empty_for_whitespace_only` | `"   "`, `"\t\n"` → `""` |
| `test_strips_leading_trailing_whitespace` | `"  sk-abc  "` → `"sk-abc"` |
| `test_strips_bearer_prefix` | `"Bearer sk-abc"` → `"sk-abc"` |
| `test_strips_bearer_prefix_case_insensitive` | `BEARER` / `bearer` / `BeArEr` — все работают |
| `test_strips_authorization_header_prefix` | `"Authorization: Bearer sk-abc"` → `"sk-abc"` |
| `test_strips_authorization_case_insensitive` | Любой регистр `Authorization:` |
| `test_preserves_valid_token_unchanged` | Чистый `"sk-proj-..."` — без изменений |
| `test_preserves_non_openai_tokens` | GitHub и прочие токены с/без префикса тоже очищаются корректно |
| `test_handles_multiple_spaces_after_bearer` | Двойной пробел после `Bearer` не ломает |
| `test_is_single_source_not_duplicated` | Регрессия BUG-07: `main.py` импортирует функцию, не определяет свою |

### `test_schemas_validation.py` — 22 теста, покрывают **U6** (Pydantic-валидация API)

Фокус на кастомных валидаторах и доменных ограничениях, которых Pydantic сам не знает.

| Группа | Тесты |
|--------|-------|
| `StartJobRequest` | пустой `analysis_columns` → ошибка / `parallelism > GLOBAL_LLM_PARALLELISM` → ошибка с упоминанием лимита / граничное значение — OK / 0 или отрицательный parallelism → ошибка / `temperature` вне [0, 2] → ошибка / `max_reviews=0` → ошибка / `analysis_mode="sentiment"` → ошибка (только `custom`) |
| `AnalysisOutput` | валидный payload → OK / неизвестный `sentiment_label` → ошибка / `negativity_score` вне [0, 1] → ошибка / `summary > 240` символов → ошибка |
| `JobStatus` | ровно 6 значений: queued/running/paused/completed/failed/canceled — регрессия при добавлении новых |
| `AuthRequest` | username < 3 → ошибка / password < 6 → ошибка / валидный → OK |
| `SchemaSuggestRequest` | пустой `input_columns` → ошибка / неизвестный `task_hint` → ошибка / параметризовано: `support_triage`, `classification`, `extraction`, `sentiment` → OK |

### `test_to_db_query.py` — 10 тестов, покрывают **BUG-10** (naive `?→%s` replace)

| Тест | Что проверяет |
|------|---------------|
| `test_single_placeholder_replaced` | Happy-path: один `?` → один `%s` |
| `test_multiple_placeholders_replaced` | Несколько `?` → все заменены |
| `test_no_placeholders_returns_unchanged` | SQL без плейсхолдеров возвращается как есть |
| `test_question_mark_in_single_quoted_string_preserved` | `'%wtf?%'` — `?` внутри строки не заменяется |
| `test_question_mark_in_json_literal_preserved` | `'{"q":"?"}'::jsonb` — `?` в JSON сохраняется |
| `test_question_mark_in_comment_preserved` | `-- вопрос?\n` — `?` в комментарии сохраняется |
| `test_question_mark_in_multiline_comment_preserved` | `/* ok? */` — `?` в /* */ сохраняется |
| `test_question_mark_in_double_quoted_identifier_preserved` | `"strange?col"` — `?` в идентификаторе Postgres сохраняется |
| `test_escaped_single_quote_in_string` | `'it''s'` — SQL-escape `''` не ломает парсер |
| `test_mixed_literal_and_placeholder` | Смесь: `'what?'` сохраняется + `?` заменяется на `%s` |

### `test_compute_cache_key.py` — 9 тестов, покрывают **U7** (генерация cache_key для LLM-кэша)

| Тест | Что проверяет |
|------|---------------|
| `test_returns_sha256_hex_strings` | Все 5 возвращаемых значений — 64-символьные hex-строки SHA-256 |
| `test_identical_input_produces_identical_key` | Детерминированность — одинаковые входы → один key |
| `test_json_key_order_does_not_affect_key` | `sort_keys=True` работает — перестановка ключей в `expected_json_template` не меняет key |
| `test_different_prompt_produces_different_key` | Разные промпты → разные keys |
| `test_different_review_produces_different_key` | Разные отзывы → разные keys (нет wrong-hit) |
| `test_different_provider_produces_different_key` | OpenAI vs Ollama на одном запросе → разные keys |
| `test_different_model_produces_different_key` | Разные модели → разные keys |
| `test_none_and_empty_schemas_produce_same_key` | `None` и `{}` для schema → один key (через `template or {}`) |
| `test_component_hashes_independent_of_unrelated_fields` | `prompt_hash` зависит только от `prompt_template`, не от модели/отзыва |

### `test_version.py` — 5 тестов, покрывают **BUG-11** (единый источник версии)

| Тест | Что проверяет |
|------|---------------|
| `test_version_is_defined_and_non_empty` | `app.__version__` — непустая строка |
| `test_version_matches_semver_format` | Формат `MAJOR.MINOR.PATCH` (+ опциональный пре-релиз суффикс) |
| `test_version_has_no_mvp_prefix` | Не начинается с `"mvp-"` — регрессия после выхода из MVP |
| `test_job_manager_uses_shared_version` | `job_manager.APP_VERSION == app.__version__` — единый источник |
| `test_version_is_2_x` | Major >= 2 — закрепляем релизную линию 2.x |

---

## Как писать новые тесты

### 1. Создать файл в `tests/unit/`

Называем `test_<что_тестируем>.py`. Пример:

```python
"""Юнит-тесты для foo."""

from __future__ import annotations

import pytest


def test_foo_basic_case() -> None:
    from app.foo import foo
    assert foo(1) == 2
```

### 2. Мокать внешние зависимости через `monkeypatch`

```python
def test_example(monkeypatch: pytest.MonkeyPatch) -> None:
    # Важно: патчим там, где имя используется при вызове.
    # Если в модуле X сделано `from Y import bar`, патчим X.bar, а не Y.bar.
    monkeypatch.setattr("app.consumer.bar", lambda: "mocked")
    ...
```

### 3. Маркировать медленные тесты

```python
@pytest.mark.slow
def test_large_dataset() -> None:
    ...  # занимает > 5 секунд
```

### 4. Запустить локально

```bash
cd backend
python -m pytest tests/unit/test_foo.py -v
```

### 5. Связь с документацией

При каждом **фиксе бага** или **новой функциональности** — обновлять:

- `docs/TESTING.md` (этот файл) — добавить строку в соответствующую таблицу
- [`docs/RELEASE_NOTES.md`](RELEASE_NOTES.md) — если фикс заметен пользователю

---

## Что ещё предстоит

- **Уровень 2 (сценарные, Docker):** полный пайплайн upload → отчёт → выгрузка. Требуют `testcontainers[postgres]` + `fakeredis` + `MockLLMProvider`.
- **Уровень 3 (recovery):** отказоустойчивость (kill воркера, stale lease, partition modulus, rotation).
- **Уровень реального LLM:** прогон с реальным OpenAI/Ollama на небольших файлах.

Запуск всех тестов — **только локально, в Docker**.
