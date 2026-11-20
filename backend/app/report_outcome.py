"""Понятный результат обработки без изменения машинного жизненного цикла отчёта."""
LABELS = {"queued":"В очереди", "running":"В работе", "paused":"На паузе", "failed":"Ошибка",
          "canceled":"Отменён", "completed":"Готов"}


def presentation(status, processed, failed):
    processed, failed = int(processed or 0), int(failed or 0)
    all_failed = processed > 0 and failed >= processed
    label = LABELS.get(status, status)
    hint = None
    if status == "completed" and failed:
        label = f"Ошибка: {failed} из {processed}" if all_failed else f"Готов, ошибок: {failed}"
        hint = "Проверьте доступность провайдера и время ожидания. Скачайте ошибки и повторите обработку."
    return {"status_label":label, "all_rows_failed":status == "completed" and all_failed,
            "has_errors":failed > 0, "retry_hint":hint}
