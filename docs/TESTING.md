# Development and tests

The backend unit suite runs with Python 3.12. It uses fixtures and mocks for services. The frontend build uses Node.js 20 in CI.

## Backend

From the checkout root, enter `backend` and run the CI commands.

```sh
cd backend
python -m pip install -r requirements-dev.txt
python -m pytest tests/unit -q
```

The suite covers sign-in, quotas, prompt rendering, file parsing, model retries, recovery, and report exports. Use `-m "not slow"` to omit tests marked as slow.

## Frontend

From the checkout root, enter `frontend`.

```sh
cd frontend
npm ci
npm run build
```

For Vite development against a separately running API, set `VITE_API_BASE` using [the frontend environment template](../frontend/.env.example). Add the dev server origin to `CORS_ALLOW_ORIGINS`.

## End-to-end smoke test

Run [the Compose setup](../README.md#deploy) with a separate project name and synthetic input. Confirm that a local account can sign in, load a sample file, preview a prompt, process a few rows, and open the Excel export. In the admin settings, switch between English and Russian and check that the interface, sample files, and built-in prompt follow the selected language.
