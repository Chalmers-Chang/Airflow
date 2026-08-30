# Local Airflow (Docker Compose)

Official Apache Airflow [2.10.5 docker-compose](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html) on Docker Desktop, CeleryExecutor.

Local development only. Do not use this stack in production.

## Why 2.10.5 + Python 3.8

| Item | Version | Why |
|---|---|---|
| Python | 3.8 (`>=3.8.1,<3.9`) | Project runtime |
| Apache Airflow | **2.10.5** | Last release that still supports Python 3.8. 2.11 / 3.x dropped 3.8 |
| Image | `apache/airflow:2.10.5-python3.8` | Pinned in `.env` as `AIRFLOW_IMAGE_NAME` |

Package versions are managed with **uv** at the repo root (`pyproject.toml` + `uv.lock`). The image already includes Airflow 2.10.5.

## Services

| Service | Role | Image / command | Ports | When |
|---|---|---|---|---|
| `postgres` | Metadata DB | `postgres:13` | 5432 in-network | default |
| `redis` | Celery broker | `redis:7.2-bookworm` | 6379 exposed, not published | default |
| `airflow-init` | Dirs, migrate, admin user | one-shot | — | other Airflow services wait for it |
| `airflow-webserver` | UI / REST | `webserver` | **8080** → http://localhost:8080 | default |
| `airflow-scheduler` | Schedule / parse DAGs | `scheduler` | health 8974 in-network | default |
| `airflow-worker` | Run tasks | `celery worker` | — | default |
| `airflow-triggerer` | Deferrable tasks | `triggerer` | — | default |
| `airflow-cli` | `airflow` CLI | profile `debug` | — | `docker compose --profile debug run --rm airflow-cli ...` |
| `flower` | Celery UI | profile `flower` | **5555** | `docker compose --profile flower up -d` |

### Defaults

| Item | Value |
|---|---|
| UI | http://localhost:8080 |
| User | `airflow` |
| Password | `airflow` |
| Executor | `CeleryExecutor` |
| Example DAGs | off (`AIRFLOW__CORE__LOAD_EXAMPLES: 'false'`). Project DAGs are under `dags/` |

### Volumes (`AIRFLOW_PROJ_DIR=../..`, repo root)

| Host | Container |
|---|---|
| `dags/` | `/opt/airflow/dags` |
| `logs/` | `/opt/airflow/logs` |
| `config/` | `/opt/airflow/config` |
| `plugins/` | `/opt/airflow/plugins` |
| iCloud `…/CloudDocs/airflow` (`ICLOUD_AIRFLOW_DIR`) | `/opt/airflow/icloud` (read-only) |
| host `AIRFLOW_DATA_DIR` (config / SSH / import state) | `/opt/data/airflow` |
| volume `postgres-db-volume` | Postgres data |

Official resource floor: 4 GB RAM, 2 CPUs, 10 GB disk. On macOS, `AIRFLOW_UID` is the host uid (`501` here) so `logs/` is not owned by root.

## Setup

1. Docker Desktop is running (not paused), with at least 4 GB RAM / 2 CPUs / 10 GB disk.
2. Create local env from the template (**required** — empty `ICLOUD_AIRFLOW_DIR` breaks compose):

```bash
cd airflow.deployment/docker-compose
cp .env.example .env
# edit AIRFLOW_UID (id -u), AIRFLOW_DATA_DIR, ICLOUD_AIRFLOW_DIR, IMPORT_APPLE_CALENDAR_SSH_*
bash ../../dags/import_apple_calendar/scripts/setup_host_ssh.sh
```

3. For `import_apple_calendar`, also complete macOS Remote Login. Full steps: [dags/import_apple_calendar/README.md](../../dags/import_apple_calendar/README.md).
4. Optional local Python tooling with [uv](https://docs.astral.sh/uv/):

```bash
curl -LsSf https://astral.sh/uv/install.sh | sh
```

## uv (local venv)

From the **repo root**:

```bash
uv python install 3.8
uv sync
```

Add a package:

```bash
uv add <package>
```

That updates `pyproject.toml` / `uv.lock` and the local `.venv`. It does **not** install into the Airflow containers.

### `requirements.txt` is for the container, not for uv

`airflow.deployment/docker-compose/requirements.txt` is extra `pip` packages on top of the official image. Do not list `apache-airflow`. Do not dump a full `uv export`.

| File | Who reads it | When you add a package |
|---|---|---|
| `pyproject.toml` / `uv.lock` | uv, local `.venv`, IDE | always `uv add` |
| `docker-compose/requirements.txt` | container pip | only extras the DAG imports that are missing from the image; copy `package==version` from `uv.lock` |

Then either set `_PIP_ADDITIONAL_REQUIREMENTS` in `.env` (reinstalls on every `up`; local-only) or bake them into the `Dockerfile` (`build: .`).

Official constraint file: https://raw.githubusercontent.com/apache/airflow/constraints-2.10.5/constraints-3.8.txt

## Start / stop

```bash
cd airflow.deployment/docker-compose
docker compose up -d
docker compose ps
docker compose logs -f airflow-webserver
```

UI: http://localhost:8080 — `airflow` / `airflow`. New DAGs start paused.

```bash
docker compose --profile debug run --rm airflow-cli airflow dags list
docker compose down
docker compose down --volumes --remove-orphans
```

## DAGs in this repo

- `dags/mysql_archive_jobs/`
- `dags/import_apple_calendar/` (`pypdf`, `caldav`, `icalendar` must be on the image; see `requirements.txt`)

## Related

- [Running Airflow in Docker (2.10.5)](https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html)
- [Supported versions](https://airflow.apache.org/docs/apache-airflow/2.10.5/installation/supported-versions.html)
