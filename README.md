# Airflow

DAGs and a **local** Apache Airflow stack. Python **3.8** is the project runtime, so Airflow is pinned to **2.10.5** (the last release that still supports 3.8). Do not use this compose file in production.

## Versions

| Item | Version | Where it is pinned |
|---|---|---|
| Python | 3.8 (`>=3.8.1,<3.9`) | `pyproject.toml` `requires-python`; image tag `-python3.8` |
| Apache Airflow | **2.10.5** | `pyproject.toml`; `AIRFLOW_IMAGE_NAME` |
| Docker image | `apache/airflow:2.10.5-python3.8` | `airflow.deployment/docker-compose/.env` |
| Executor | CeleryExecutor | `docker-compose.yaml` |
| Postgres (metadata) | 13 | `docker-compose.yaml` |
| Redis (Celery broker) | 7.2-bookworm | `docker-compose.yaml` |
| Extra DAG packages | `pypdf==4.3.1`, `caldav==1.3.9`, `icalendar==5.0.14` | `.env` `_PIP_ADDITIONAL_REQUIREMENTS` and `requirements.txt` |

Airflow 2.11 and 3.x dropped Python 3.8. Official constraints: [constraints-2.10.5 / 3.8](https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt).

Local package pins live in `pyproject.toml` + `uv.lock` ([uv](https://docs.astral.sh/uv/)). That venv is for the IDE / local checks. Containers already have Airflow; they only pip-install the extras above.

## Deploy (Docker Compose, this Mac)

1. Install and start **Docker Desktop**. Give it at least 4 GB RAM, 2 CPUs, 10 GB disk (Airflow’s official local floor). Keep it **unpaused**.
2. Create `airflow.deployment/docker-compose/.env` from the template:

```bash
cd airflow.deployment/docker-compose
cp .env.example .env
```

   Edit at least:
   - `AIRFLOW_UID` = `id -u` (often `501` on macOS)
   - `ICLOUD_AIRFLOW_DIR` = host iCloud `airflow` folder
   - `IMPORT_APPLE_CALENDAR_SSH_USER` / `IMPORT_APPLE_CALENDAR_MATERIALIZE_SCRIPT` (for cloud-only PDF download)
3. From the compose directory:

```bash
docker compose up -d
docker compose ps
```

4. UI: http://localhost:8080 — user `airflow` / password `airflow` (local defaults). New DAGs start **paused**.
5. For `import_apple_calendar` (Remote Login, SSH key, `source_config.json`): see [dags/import_apple_calendar/README.md](dags/import_apple_calendar/README.md).
6. Stop:

```bash
docker compose down
```

Wipe the metadata DB as well: `docker compose down --volumes --remove-orphans`.

More services, uv, and `requirements.txt` vs lockfile: [airflow.deployment/docker-compose/README.md](airflow.deployment/docker-compose/README.md). Official guide: [Running Airflow in Docker (2.10.5)](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html).

## DAGs

| Path | What |
|---|---|
| [dags/mysql_archive_jobs/](dags/mysql_archive_jobs/README.md) | MSSQL / MySQL sync and house keeping |
| [dags/import_apple_calendar/](dags/import_apple_calendar/README.md) | Crew-report PDFs → iCloud calendar |

Shared Airflow Variables are created on parse (`dags/common/variables.py`); existing values are never overwritten.
