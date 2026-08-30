# docker-compose

## GOTCHA

- Missing `.env` / empty `ICLOUD_AIRFLOW_DIR` → `invalid spec: :/opt/airflow/icloud:ro: empty section between colons` on `docker compose up|down`. Always `cp .env.example .env` and set `ICLOUD_AIRFLOW_DIR`, `AIRFLOW_UID`, `AIRFLOW_DATA_DIR`, and `IMPORT_APPLE_CALENDAR_SSH_*`.
- Host `/opt/data/...` is not shared by Docker Desktop by default (`Mounts denied`). Prefer `AIRFLOW_DATA_DIR` under `$HOME` (example uses `~/Library/Application Support/airflow`); container always sees `/opt/data/airflow`.
- Official `apache/airflow:2.10.5-python3.8` does not include `pypdf` / `caldav` / `icalendar`. `import_apple_calendar` will fail to parse until those extras are installed (`_PIP_ADDITIONAL_REQUIREMENTS` or a custom image).
- `_PIP_ADDITIONAL_REQUIREMENTS` reinstalls on every container start. Fine for local; use a Dockerfile for anything longer-lived.
- Do not put `apache-airflow` in compose `requirements.txt`.
- `uv sync` of the repo `pyproject.toml` on macOS ARM + CPython 3.8 can fail compiling `google-re2` (Airflow dependency). The Docker image already has Airflow; local full-venv sync is optional.

## TASTE

- Pin `AIRFLOW_IMAGE_NAME=apache/airflow:2.10.5-python3.8` and `AIRFLOW_PROJ_DIR=../..` in `.env` (start from `.env.example`).
- `uv add` at repo root; copy only extra pins into `requirements.txt` for the container.
- Mount the iCloud `airflow` folder (not a single PDF) at `/opt/airflow/icloud:ro` so extra people are just more `data/<name>/` dirs + local `source_config.json` rows.
