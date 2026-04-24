# Branding

Как работает логотип:

- `branding/logo.png` - логотип проекта (приоритет 1).
- `branding/logo.svg` - резервный SVG-логотип (приоритет 2).
- `frontend/public/logo-default.svg` - дефолтный логотип из репозитория.

Логика в UI:
- сначала пробует `/branding/logo.png`;
- если его нет - пробует `/branding/logo.svg`;
- если его нет - пробует `/branding/logo-default.png`;
- если файла нет, автоматически показывает дефолтный `/logo-default.svg`.

Как обновить логотип:
1. Замените `branding/logo.svg` и/или `branding/logo.png`.
2. Перезапустите frontend: `docker compose up -d --build frontend`.
