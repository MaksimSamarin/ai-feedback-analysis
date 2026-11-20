English · [Русский](CONFIGURATION.ru.md) · [Analyzer](../README.md)

# Configuration

Copy [`.env.example`](../.env.example) to `.env` beside [`docker-compose.yml`](../docker-compose.yml). Compose reads that file when you run commands from the checkout root.

## First start

| Setting | What to enter |
| --- | --- |
| `APP_SECRET` | A long random string. Analyzer uses it to encrypt saved model API keys. Keep the value when you upgrade and across API and worker replicas. |
| `POSTGRES_PASSWORD` | The password for the PostgreSQL account set in `POSTGRES_USER`. |
| `DATABASE_URL` | The address and credentials Analyzer uses to connect to PostgreSQL. For the bundled database, use host `postgres` and port `5432`. Match the user, password, and database name to the `POSTGRES_*` settings. |

Generate independent values for `APP_SECRET` and `POSTGRES_PASSWORD` using the [command in the deployment steps](../README.md#deploy). Enter the database password in both database settings.

```dotenv
APP_SECRET=<random-app-secret>
POSTGRES_USER=review_analyzer
POSTGRES_DB=review_analyzer
POSTGRES_PASSWORD=<postgres-password>
DATABASE_URL=postgresql://review_analyzer:<postgres-password>@postgres:5432/review_analyzer
```

A generated hexadecimal password can be copied directly into the URL. For an existing password with reserved URL characters, URL-encode the password in `DATABASE_URL`. Keep the raw password in `POSTGRES_PASSWORD`.

## Model connection

For an **OpenAI-compatible API**, enter its base URL and model IDs. The base URL accepts a trailing `/v1`. For a hosted endpoint, use its URL and API key. You can also enter the API key in the interface before running an analysis.

Here is an example for a local model server running on the host at port 8081 in Docker Desktop. Replace the port and model ID with those of your server. Use `local` as the key value when the endpoint accepts requests without authentication.

```dotenv
OPENAI_BASE_URL=http://host.docker.internal:8081/v1
OPENAI_MODELS=my-local-model
OPENAI_API_KEY=local
```

For **Ollama**, enter its URL and an installed model ID, then select Ollama in Analyzer.

```dotenv
OLLAMA_BASE_URL=http://host.docker.internal:11434
OLLAMA_MODELS=llama3.1:latest
```

Use an address reachable from the containers. On Linux, a LAN address or a shared Docker network alias can serve this purpose. The model lists seed the catalog during the first start. Manage existing entries in **Administration → Models**.

## Access and web address

| Setting | Value |
| --- | --- |
| `FRONTEND_PORT` | The port you open in the browser. The template uses `8080`. |
| `FRONTEND_BIND_ADDRESS` | The host interface to listen on. The template binds to `127.0.0.1`. |
| `CORS_ALLOW_ORIGINS` | Comma-separated frontend origins for clients served separately from the API. The regular Compose setup serves the UI and API on one origin. |
| `BOOTSTRAP_ADMIN_USERNAME`, `BOOTSTRAP_ADMIN_PASSWORD` | Set both before first start to create a local administrator. Use at least eight characters with a letter and a digit for the password. |
| `LOCAL_AUTH_ENABLED` | `1` enables password sign-in. |
| `LOCAL_REGISTRATION_ENABLED` | With password sign-in enabled, leave blank to allow registration when SSO is off. Use `1` or `0` to choose explicitly. |
| `SSO_*`, `SESSION_COOKIE_SECURE` | Follow the [OpenID Connect setup](AUTHENTICATION.md) for the issuer, callback, and HTTPS settings. |

## Processing and storage settings

| Setting | Value in the template |
| --- | --- |
| `REQUEST_TIMEOUT_SEC` | `180` seconds for a model request. |
| `GLOBAL_LLM_PARALLELISM` | `12` in-flight model calls per analysis worker. |
| `GROUP_MAX_ROWS` | `100` rows per analysis group. |
| `MAX_UPLOAD_SIZE_MB` | `200` for XLSX or CSV. Align the request-body limit in [Nginx](../frontend/nginx.conf) if you change it. |
| `REPORT_KEEP_LAST`, `REPORT_CLEANUP_INTERVAL_SEC` | Retain the latest `20` reports per user and run cleanup every `600` seconds. Set the interval to `0` to disable report cleanup. |
| `PAUSED_REPORT_TTL_DAYS` | `0` disables automatic expiry for paused reports. |
| `SEMANTIC_CACHE_ENABLED` | `1` enables semantic matching with an embeddings endpoint. Use `0` for exact-match caching only. |

[The environment template](../.env.example) lists the other settings. Check the resolved configuration and apply changes to the affected containers.

```sh
docker compose config --quiet
docker compose up -d --wait
```

For a scaled deployment, apply changes using your usual `--scale` flags or the Compose override.

## Hardware and scaling

### Starting configuration

Start with one instance of each Compose service. The default launch runs **six containers in total**, including one analysis worker and one file inspection worker.

```sh
docker compose up -d --build --wait
```

When the model runs on a separate endpoint, **8 vCPUs and 16 GB of RAM** are a starting point for Analyzer. The per-container limits in [`docker-compose.yml`](../docker-compose.yml) add up to **6.25 CPUs and about 11.4 GiB of RAM** for the default launch. If the model runs on the same host, budget its compute and memory separately.

| Service | Containers | CPU limit per container | Memory limit per container |
| --- | ---: | ---: | ---: |
| PostgreSQL | 1 | 3.0 | 4 GiB |
| Redis | 1 | 0.5 | 1 GiB |
| API | 1 | 0.75 | 1,500 MiB |
| Analysis worker | 1 | 1.0 | 2,500 MiB |
| File inspection worker | 1 | 0.75 | 2 GiB |
| Frontend | 1 | 0.25 | 512 MiB |

Start with **200 GB for data** and expand the volume as PostgreSQL, uploads, exports, and the response cache grow. Size the volume around the amount of data and the retention period.

### Add capacity when needed

Check the analysis queue and provider limits. If analysis jobs keep waiting and the host and model provider have headroom, add a worker.

```sh
docker compose up -d --wait --scale worker=2
```

With the other services at one replica each, this runs **seven containers in total**. The second analysis worker adds a limit of 1 CPU and 2,500 MiB of RAM. `GLOBAL_LLM_PARALLELISM` sets the request concurrency of each analysis worker. The template uses 12.

Scale `inspect-worker` if files wait for inspection. If you change the number of `backend` replicas, run `docker compose restart frontend` to refresh the backend addresses in Nginx. Keep one frontend for the published host port.

### Save the replica count

To keep two analysis workers across deployments, create `compose.scale.yml` beside `docker-compose.yml`. Set `scale` to the replica count. Use `cpus` or `mem_limit` in the same service entry to override its per-container limits.

```yaml
services:
  worker:
    scale: 2
```

```sh
docker compose -f docker-compose.yml -f compose.scale.yml up -d --build --wait
```
