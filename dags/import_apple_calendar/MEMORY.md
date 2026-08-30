# import_apple_calendar

## GOTCHA

- Docker bind-mount of iCloud Drive can raise `OSError: [Errno 5] Input/output error` when reading a cloud-only PDF. Treat that file as unread: keep the previous `mtime` so it is not “removed”; retry new unread files next run. Do not fail the whole task.
- Change detection uses file `mtime` (not sha256). Record `mtime` only after a successful calendar write (`written == len(events)`; includes 0-event PDFs that finished the write path). Dry-run never updates state.
- Docker cannot materialize iCloud files. `dd` of a dataless PDF can exit 0 with 0 bytes; detect with `sha256sum` (EIO). Task SSHs to `host.docker.internal` and runs `scripts/materialize_icloud.sh` (10s × 18, then exit). No LaunchAgent. Remote Login was **off** (port 22 refused, no sshd) on 2026-08-30; enable it then `scripts/setup_host_ssh.sh`. Host `dd`/`brctl download` clears `dataless` so Docker can `sha256sum`; setup alone is not enough while port 22 is closed. Docker Desktop paused also blocks worker SSH tests.
- SSH BatchMode PATH is `/usr/bin:/bin:/usr/sbin:/sbin` only → `docker` not found → script prints `airflow-worker not running` even when workers are up. Fix: prepend `/usr/local/bin:/opt/homebrew/bin` in `materialize_icloud.sh`. With multiple worker replicas, take `head -n 1` for `docker exec` checks.
- Empty / missing `IMPORT_APPLE_CALENDAR_DRY_RUN` defaults to write (`"0"`). Set `"1"` for log-only. Existing Airflow Variable values are never overwritten by `ensure_all_project_variables()`.
- Do **not** enable Public Calendar. Owner CalDAV + Calendar.app sharing is enough for Mac / iPhone / invited people.
- Apple ID login password will not work. Use an **app-specific password**. The Variable is `ICLOUD_CALDAV_PASSWORD`.
- Match `calendar_name` to the Calendar.app sidebar string exactly (`Britney duty`).
- Do not `uv export` the full lockfile into compose `requirements.txt`. Only extra packages: `pypdf`, `caldav`, `icalendar`.
- Pin extras to Python 3.8: `pypdf>=4.3.1,<5`, `caldav>=1.3.9,<1.4`, `icalendar>=5.0.13,<6`.
- Local setup (config, SSH keys, import state) lives under host `AIRFLOW_DATA_DIR` (default `~/Library/Application Support/airflow`), mounted in containers at `/opt/data/airflow`. Do not store these under the git checkout — `git pull` must not wipe them. Docker Desktop cannot mount host `/opt/data` unless File Sharing is added.
- SSH remote command must `shlex.quote` PDF paths; filenames with spaces otherwise split (`Crew Report_2608 2.pdf` → three bogus targets).
- Hardcoded Mac username / repo path in code broke new machines. SSH user + materialize script path come only from compose `.env` (`IMPORT_APPLE_CALENDAR_SSH_*`); no machine-specific defaults.
- Commit only `source_config.json.example`. Runtime config is `/opt/data/airflow/import_apple_calendar/source_config.json`. Missing file → clear `FileNotFoundError` with copy instructions.

## TASTE

- `source_config.json` lists people/calendars under `/opt/data/airflow/import_apple_calendar/` (from `.example` via `setup_host_ssh.sh`). Secrets stay in Airflow Variables; parse-time `ensure_all_project_variables()` creates missing keys and never overwrites.
- Idempotent UID: `import-apple-calendar-{source_id}-{crew_id}-{date}`. Prefer `event_by_uid` then `save_event`.
- Default `IMPORT_APPLE_CALENDAR_DRY_RUN=0` (write). Set `"1"` for log-only.
- Britney’s PDFs: `/opt/airflow/icloud/data/britney.duty.schedule`. Calendar target: sidebar name `Britney duty`.
- Skip `休息日` / `例假日` / `休假日` by requiring both check-in and check-out as real times (`0:00`/`00:00` ignored). Store written UIDs under `/opt/data/airflow/import_apple_calendar/{source_id}.json` with crew_id + month. Delete only that crew’s months from the changed file — uploading August after September must not drop September. Skip files whose `mtime` is unchanged. Schedule `*/5 * * * *`.
