# ETL DAGs (`mysql_archive_jobs/`)

This folder is what you deploy to Airflow. Treat **`mysql_archive_jobs/`** as `dags` (or copy the whole folder into Airflow's dags path).

The three DAG files sit at this folder's root so they are easy to find in Airflow.

This project syncs MSSQL / MySQL into a target MySQL and runs house keeping on synced source rows.

`class` is a Python reserved word. Shared objects live in `classes/`.

## Layout

```
mysql_archive_jobs/
├── config/                              # settings and connection accounts
│   ├── appsetting.py                    # Slack, DAG ids, schedules, table names, retries
│   ├── db_config.py                     # POC / PROD host, db, credentials, proxy
│   ├── convert_rule.json               # MSSQL → MySQL column type conversion rules
│   ├── etl_task_record.csv              # MySQL → MySQL task list
│   └── etl_task_record_sp.csv           # MSSQL → MySQL task list
├── classes/                             # shared objects
│   ├── sync_config.py                   # MSSQL→MySQL SyncConfig (includes connections)
│   ├── etl_task_config.py               # MySQL task connection config
│   └── sharding.py                      # shard log / timestamp_tag
├── sql/                                 # SQL builders only; not DAGs
│   ├── common.py                        # information_schema table-exists checks
│   ├── mssql/
│   │   ├── stored_procedure.py          # EXEC stored procedure
│   │   ├── control.py                   # etl_task_record_sp control table
│   │   └── load.py                      # upsert into target MySQL
│   └── mysql/
│       ├── sync_query.py                # source SELECT by etl_type
│       ├── control.py                   # etl_task_record / etl_sync_ids
│       ├── schema.py                    # columns, comments, shard tables, indexes
│       ├── sharding_log.py              # sharding_table_log
│       ├── load.py                      # shard upsert / truncate insert
│       └── house_keeping.py             # delete expired rows, OPTIMIZE
├── mssql_to_mysql_sync_jobs.py          # DAG: mssql_to_mysql_sync_tasks
├── mysql_to_mysql_sync_jobs.py           # DAG: mysql_to_mysql_sync_tasks
├── mysql_house_keeping.py               # DAG: myssql_house_keeping_tasks
├── slack/
│   ├── Slack.py
│   └── notifier.py
└── general/
    └── toolbox.py                       # password_decode, safe_sql_value
```

`.airflowignore` skips `config/`, `classes/`, `sql/`, `slack/`, `general/`. Real DAGs live at the `mysql_archive_jobs/` root.

Change SQL in `sql/` first. Do not put long SQL back into DAG files. DAG files only handle connections, loops, and Airflow tasks.

## The three DAGs

| DAG id (do not rename casually) | File | Schedule (UTC+0) | Purpose |
|---|---|---|---|
| `mssql_to_mysql_sync_tasks` | `mssql_to_mysql_sync_jobs.py` | `0 7 * * *` | Call MSSQL stored procedures and batch-write MySQL |
| `mysql_to_mysql_sync_tasks` | `mysql_to_mysql_sync_jobs.py` | `0 17 * * *` | MySQL → target MySQL; sharding and sync-id tracking |
| `myssql_house_keeping_tasks` | `mysql_house_keeping.py` | `0 22 * * *` | Delete expired source rows via `etl_sync_ids_*`; optional OPTIMIZE |

Schedules, DAG ids, Slack, table names, and retry counts live in `config/appsetting.py`.

## POC / PROD

Airflow Variable `is_poc`:

- `"1"` → `POC_MySQL_account` in `config.db_config`
- anything else → `PROD_MySQL_account`

Accounts live in `config/db_config.py`. Passwords are Fernet tokens. Decrypt with Variable `crypto_key` (`general.toolbox.password_decode`).

House keeping forces `proxy = None` on POC. PROD uses `db_config.proxy`. The other two DAGs always use the proxy from the account config for Slack.

## Airflow Variables

DAG parse creates any **missing** keys with defaults (`dags/common/variables.py`) and never overwrites values you already set. Fill them in the UI.

| Variable | Used by | Meaning |
|---|---|---|
| `is_poc` | all | `"1"` POC, otherwise PROD |
| `crypto_key` | all | Fernet decrypt key |
| `airflow_task_id` | all | Filters control-table rows by `airflow_task_id` |
| `is_config_table_updated` | MSSQL DAG | `"1"` rebuilds/updates `etl_task_record_sp` from CSV |
| `is_config_updated` | MySQL→MySQL DAG | `"1"` rebuilds/updates `etl_task_record_{id}` and `etl_sync_ids_{id}` from CSV |
| `is_optimization_active` | house keeping | `1` runs OPTIMIZE |
| `optimize_start_time_str` | house keeping | e.g. `00:00`, timezone UTC+8 |
| `optimize_end_time_str` | house keeping | skip OPTIMIZE after this time |
| `mysql_house_keeping_date_sub_start_day` | house keeping | delete-window start (days ago) |
| `mysql_house_keeping_date_sub_end_day` | house keeping | delete-window end |

Slack channel / token live in `config/appsetting.py` as `SLACK_CHANNEL` and `SLACK_TOKEN`.

## Control tables and CSV

### MSSQL → MySQL

- Control table: `etl_task_record_sp`
- Seed file: `config/etl_task_record_sp.csv`
- SQL: `sql/mssql/control.py`, `sql/mssql/stored_procedure.py`, `sql/mssql/load.py`
- Set `is_config_table_updated` to `"1"`. Next parse CREATE (if missing) and UPSERT CSV, then set the Variable back to `"0"`
- `etl_type`: `1` by id, `2` by timestamp, `4` full table in batches (does not write last id)

### MySQL → MySQL

- Control table: `etl_task_record_{airflow_task_id}`
- Synced ids: `etl_sync_ids_{airflow_task_id}` (zlib; used by house keeping)
- Shard log: `sharding_table_log`
- Seed file: `config/etl_task_record.csv`
- SQL: `sql/mysql/sync_query.py`, `control.py`, `schema.py`, `sharding_log.py`, `load.py`
- Set `is_config_updated` to `"1"` to load from CSV
- `etl_type`: `0` skip, `1` id, `2` timestamp, `3` lagged date (default 3 days), `4` truncate then full load

### House keeping

Reads the same `etl_task_record_{airflow_task_id}` where `is_active = 1`, `etl_type not in (0, 4)`, and `if_need_house_keeping = 1`. SQL is in `sql/mysql/house_keeping.py`. Deletes rows that are already synced and whose source timestamp is older than N days (default 14).

Keep passwords in CSV as encrypted tokens.

## Add a sync table

1. Add a row to the matching CSV (`etl_task_record_sp.csv` for MSSQL, `etl_task_record.csv` for MySQL).
2. Match `airflow_task_id` to the Variable.
3. Set `is_config_table_updated` or `is_config_updated` to `"1"`.
4. Wait for DAG parse / starting stage and confirm the control table updated.
5. For MSSQL type quirks, add a rule in `config/convert_rule.json` (`bit` / `bigint`).

## Where to change things

| What | File |
|---|---|
| Host / db / user / encrypted password / proxy | `config/db_config.py` |
| Slack, schedule, DAG id, table names, retries | `config/appsetting.py` |
| Connection and task objects | `classes/` |
| SQL | `sql/mssql/` or `sql/mysql/` (by function) |
| Slack send | `slack/Slack.py` |
| MSSQL sync flow | `mssql_to_mysql_sync_jobs.py` |
| MySQL sync / sharding flow | `mysql_to_mysql_sync_jobs.py` |
| Source delete and OPTIMIZE | `mysql_house_keeping.py` |

After edits, confirm the DAG still parses in the Airflow UI.

## Maintenance notes

- Changing a DAG id that already ran in production creates a new DAG.
- House-keeping DAG id stays `myssql_house_keeping_tasks` (historical typo).
- Decrypt failures usually mean `crypto_key` does not match, or the CSV / `db_config` value is not a Fernet token.
- Parse queries the MySQL control table to build tasks. If that connection fails, the DAG does not show in the UI.
- Local imports need cwd and Python path set to `mysql_archive_jobs/`.
