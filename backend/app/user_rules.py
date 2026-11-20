"""Пользовательские правила с фактическими настройками текущего контура."""
from pathlib import Path
from app.config import MAX_UPLOAD_BYTES, PAUSED_REPORT_TTL_DAYS

RULES_PATH = Path(__file__).with_name('user_rules.md')


def user_rules_content(language: str = "ru") -> str:
    days = PAUSED_REPORT_TTL_DAYS
    path = RULES_PATH
    if language == "en":
        path = path.with_name("user_rules_en.md")
        unit = "day" if days == 1 else "days"
        policy = (f"Reports paused for more than {days} {unit} are canceled automatically."
                  if days else "Paused reports have no expiration.")
    else:
        ending = 'дня' if days % 10 == 1 and days % 100 != 11 else 'дней'
        policy = (f'Отчёты на паузе дольше {days} {ending} автоматически отменяются.' if days
                  else 'Автоматическая отмена долгой паузы сейчас отключена.')
    return (
        path.read_text(encoding='utf-8')
        .replace('{{upload_limit_mb}}', str(MAX_UPLOAD_BYTES // (1024 * 1024)))
        .replace('{{paused_policy}}', policy)
    )
