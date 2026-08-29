# import_apple_calendar

## GOTCHA

- Docker bind-mount of iCloud Drive can raise `OSError: [Errno 5] Input/output error` when hashing/parsing a cloud-only PDF. Treat that file as unread: keep the previous sha256 so it is not “removed”; retry new unread files next run. Do not fail the whole task.
- Docker cannot materialize iCloud files. `dd` of a dataless PDF can exit 0 with 0 bytes; detect with `sha256sum` (EIO). Task SSHs to `host.docker.internal` and runs `scripts/materialize_icloud.sh` (10s × 18, then exit). No LaunchAgent. Remote Login was **off** (port 22 refused, no sshd) on 2026-08-30; enable it then `scripts/setup_host_ssh.sh`. Docker Desktop paused also blocks worker SSH tests.
- Empty `IMPORT_APPLE_CALENDAR_DRY_RUN` used to be treated as write (`!= "1"`). Treat only `"0"` as write; empty stays dry-run. `ICLOUD_CALDAV_PASSWORD` must be filled in the UI before `"0"`.
- Do **not** enable Public Calendar. Owner CalDAV + Calendar.app sharing is enough for Mac / iPhone / invited people.
- Apple ID login password will not work. Use an **app-specific password**. The Variable is `ICLOUD_CALDAV_PASSWORD`.
- Match `calendar_name` to the Calendar.app sidebar string exactly (`Britney duty`).
- Do not `uv export` the full lockfile into compose `requirements.txt`. Only extra packages: `pypdf`, `caldav`, `icalendar`.
- Pin extras to Python 3.8: `pypdf>=4.3.1,<5`, `caldav>=1.3.9,<1.4`, `icalendar>=5.0.13,<6`.
- Local `uv sync` of `apache-airflow==2.10.5` on macOS ARM Python 3.8 can fail building `google-re2`. Parser checks can use a tiny venv with `pypdf` only.

## TASTE

- `source_config.json` lists people/calendars. Secrets stay in Airflow Variables; parse-time `ensure_all_project_variables()` creates missing keys and never overwrites.
- Idempotent UID: `import-apple-calendar-{source_id}-{crew_id}-{date}`. Prefer `event_by_uid` then `save_event`.
- Default `IMPORT_APPLE_CALENDAR_DRY_RUN=1` until Apple ID email and app password are set.
- Britney’s PDFs: `/opt/airflow/icloud/data/britney.duty.schedule`. Calendar target: sidebar name `Britney duty`.
- Skip `休息日` / `例假日` / `休假日` by requiring both check-in and check-out as real times (`0:00`/`00:00` ignored). Store written UIDs in `dags/logs/import_apple_calendar/{source_id}.json` with crew_id + month. Delete only that crew’s months from the changed file — uploading August after September must not drop September. Skip unchanged files. Schedule `*/5 * * * *`.
