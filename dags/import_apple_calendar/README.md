# import_apple_calendar

Scan a folder of **crew report PDFs**, parse duty days, and upsert events into a **dedicated iCloud calendar** (the one in Calendar.app’s iCloud list, e.g. `Britney duty` — not the personal calendar). Sharing that calendar in Calendar.app is what reaches Mac / iPhone / family. Leave **Public Calendar** off; the DAG logs in as the calendar owner over CalDAV.

v1 scans an iCloud Drive folder via `/opt/airflow/icloud` (compose `ICLOUD_AIRFLOW_DIR`). `example/` stays in the repo as a sample PDF.

This DAG only works on a **Mac host** with Docker Desktop (iCloud Drive + Remote Login). It is local-dev only.

## New machine checklist (do in order)

Work from the **repo root** unless a step says otherwise.

### 1. Docker Desktop

1. Install and start [Docker Desktop](https://www.docker.com/products/docker-desktop/).
2. Give Docker at least **4 GB RAM**, **2 CPUs**, **10 GB disk**.
3. Keep Docker **running** (not paused). Paused Desktop blocks workers and SSH materialize tests.

### 2. Compose `.env` (required or `docker compose` fails)

```bash
cd airflow.deployment/docker-compose
cp .env.example .env
```

Edit `.env`:

| Variable | How to set |
|---|---|
| `AIRFLOW_UID` | Run `id -u` on this Mac (often `501`) |
| `ICLOUD_AIRFLOW_DIR` | Absolute path to your iCloud Drive `airflow` folder (see §3) |
| `IMPORT_APPLE_CALENDAR_SSH_USER` | Your macOS short username (`whoami`) |
| `IMPORT_APPLE_CALENDAR_MATERIALIZE_SCRIPT` | Absolute path to `dags/import_apple_calendar/scripts/materialize_icloud.sh` on this Mac |

Empty `ICLOUD_AIRFLOW_DIR` causes:

`invalid spec: :/opt/airflow/icloud:ro: empty section between colons`

After editing `.env`, always recreate containers so env reaches workers:

```bash
docker compose up -d
```

### 3. iCloud Drive folder (PDF source)

1. In Finder, open **iCloud Drive** and create (or sync) a folder named `airflow`.
2. Put crew PDFs under `airflow/data/<person>/`, e.g. `airflow/data/britney.duty.schedule/*.pdf`.
3. Typical host path:

`/Users/<you>/Library/Mobile Documents/com~apple~CloudDocs/airflow`

4. That path is what you put in `ICLOUD_AIRFLOW_DIR`. Inside containers it is `/opt/airflow/icloud` (read-only).

**Cloud-only / dataless files:** Docker cannot download iCloud placeholders. If a PDF shows as cloud-only (Finder cloud icon, or `ls -lO` shows `dataless`), the DAG will SSH to this Mac and run `scripts/materialize_icloud.sh`. That needs the next section.

### 4. macOS permissions (Remote Login + SSH key)

You must do this yourself; no agent is required.

1. **System Settings → General → Sharing → Remote Login → On**
   - Allow access for your user (the same name as `IMPORT_APPLE_CALENDAR_SSH_USER`)
   - Confirm port 22 is listening: `nc -z 127.0.0.1 22 && echo ok`
2. From the **repo root**, create the worker key and append it to your `~/.ssh/authorized_keys`:

```bash
bash dags/import_apple_calendar/scripts/setup_host_ssh.sh
```

3. The script prints the `IMPORT_APPLE_CALENDAR_*` lines to put in `.env` if you have not set them yet. Then:

```bash
cd airflow.deployment/docker-compose
docker compose up -d
```

4. Optional smoke test (same key the worker uses):

```bash
KEY=dags/logs/import_apple_calendar/ssh/id_ed25519
ssh -i "$KEY" -o BatchMode=yes -o IdentitiesOnly=yes "$(whoami)"@127.0.0.1 'echo ok'
```

Keys live under `dags/logs/import_apple_calendar/ssh/` (gitignored). Never commit them.

### 5. Local calendar config (not committed)

```bash
cp dags/import_apple_calendar/config/source_config.json.example \
   dags/import_apple_calendar/config/source_config.json
```

Edit `source_config.json`: set `icloud_account_email`, `calendar_name` (exact Calendar.app sidebar name), `scan_dir`, etc. This file is gitignored because it contains account identifiers.

### 6. Apple ID / Calendar / Airflow Variables

1. Apple ID that **owns** the target calendar: 2FA on; create an [app-specific password](https://appleid.apple.com/) (not your login password).
2. Dedicated calendar (e.g. `Britney duty`). Share via Calendar.app invitations. Do **not** enable Public Calendar.
3. Start Airflow if needed (`docker compose up -d`), open http://localhost:8080 (`airflow` / `airflow`).
4. **Admin → Variables** (DAG parse creates missing keys; existing values are never overwritten):
   - `ICLOUD_CALDAV_PASSWORD` — app-specific password
   - `IMPORT_APPLE_CALENDAR_DRY_RUN` — `0` write (default for new keys); `1` log only

### 7. First run

1. Prefer `IMPORT_APPLE_CALENDAR_DRY_RUN=1` once to confirm parse counts in task logs.
2. Set dry-run to `0`, unpause `import_apple_calendar`, trigger.
3. Confirm events on Mac / iPhone under the target calendar.

`mtime` is stored only after a successful calendar write. Dry-run does not update skip state.

## Troubleshooting (symptoms from a fresh Mac deploy)

| Symptom | Fix |
|---|---|
| `invalid spec: :/opt/airflow/icloud:ro` | Copy `.env.example` → `.env` and set `ICLOUD_AIRFLOW_DIR` |
| `Errno 35` / `Input/output error` / cloud-only PDF | Enable Remote Login; run `setup_host_ssh.sh`; set SSH env in `.env`; recreate compose |
| `no SSH key at .../ssh/id_ed25519` | Run `scripts/setup_host_ssh.sh` |
| `airflow-worker not running` from materialize (workers actually up) | Fixed in script PATH; pull latest `materialize_icloud.sh`. SSH sessions need `/usr/local/bin` on PATH |
| `IMPORT_APPLE_CALENDAR_SSH_USER is empty` | Set it in `.env`, then `docker compose up -d` |
| Connection refused to port 22 | Remote Login still off, or Docker Desktop paused |
| Every run re-parses all PDFs | Dry-run does not save state; set `IMPORT_APPLE_CALENDAR_DRY_RUN=0` after a good write |
| Missing `source_config.json` | Copy from `source_config.json.example` |

## `source_config.json`

Each object in `sources` is one person’s roster → one iCloud calendar. Add another object for someone else (`calendar_name` + `scan_dir`; they can share `ICLOUD_CALDAV_PASSWORD` if the same Apple ID owns both calendars).

| Field | Meaning |
|---|---|
| `id` | Stable id (used in event UIDs). Do not rename casually. |
| `enabled` | Skip when `false` |
| `scan_dir` | Relative to this package, or an absolute path inside the container |
| `file_glob` | Default `*.pdf` |
| `parser` | v1: `crew_report` only |
| `icloud_account_email` | Apple ID email (CalDAV login) |
| `password_variable` | Airflow Variable name for the Apple app-specific password |
| `calendar_name` | Exact sidebar name in Calendar.app, e.g. `Britney duty` |
| `timezone` | Default `Asia/Taipei` |
| `event_title_prefix` | Prefix so events are easy to spot, e.g. `[Britney]` |

Event UID is `import-apple-calendar-{source_id}-{crew_id}-{date}`, so reruns update the same day instead of duplicating.

## Layout

```
import_apple_calendar/
├── import_apple_calendar.py   # DAG
├── calendar_client.py         # iCloud CalDAV upsert
├── config/
│   ├── appsetting.py
│   ├── source_config.json.example  # committed template
│   ├── source_config.json          # local only (gitignored)
│   └── secret.py
├── parsers/crew_report.py
├── host_materialize.py
├── scripts/materialize_icloud.sh
├── scripts/setup_host_ssh.sh
└── example/
```

`.airflowignore` hides `config/`, `parsers/`, `example/`, `scripts/`, `host_materialize.py`. The DAG file stays at this folder’s root.

Written event UIDs are stored in `dags/logs/import_apple_calendar/{source_id}.json` with `crew_id` and `month`. Only **new or mtime-updated files** are processed. `mtime` is recorded only after a successful calendar write (not on dry-run). Delete is limited to that file’s **Crew ID + months present in the PDF**. Unchanged mtimes are left alone.

Schedule is every 5 minutes UTC (`*/5 * * * *`). `max_active_runs=1`.

## Crew report PDF (v1)

The sample `example/Crew Report.pdf` is a duty table: date, label, check-in / check-out, flight segments. A day is added only when **both** check-in and check-out are real clock times (`0:00` / `00:00` count as missing, so rest days are skipped without label matching). Before inserting a changed file, only previously stored UIDs for that **Crew ID + those months** are deleted. Times use `Asia/Taipei`. `(+1)` on a time is the next calendar day.
