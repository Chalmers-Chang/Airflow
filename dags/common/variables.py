"""Create missing Airflow Variables with empty/default values. Never overwrite."""

from airflow.models import Variable

MYSQL_ARCHIVE_DEFAULTS = {
    "airflow_task_id": "",
    "crypto_key": "",
    "is_poc": "1",
    "is_config_updated": "0",
    "is_config_table_updated": "0",
    "is_optimization_active": "0",
    "optimize_start_time_str": "00:00",
    "optimize_end_time_str": "06:00",
    "mysql_house_keeping_date_sub_start_day": "14",
    "mysql_house_keeping_date_sub_end_day": "1",
}

IMPORT_APPLE_CALENDAR_DEFAULTS = {
    "GOOGLE_CALENDAR_API_PASSWORD": "",
    "ICLOUD_CALDAV_PASSWORD": "",
    "IMPORT_APPLE_CALENDAR_DRY_RUN": "1",
}

ALL_PROJECT_DEFAULTS = {}
ALL_PROJECT_DEFAULTS.update(MYSQL_ARCHIVE_DEFAULTS)
ALL_PROJECT_DEFAULTS.update(IMPORT_APPLE_CALENDAR_DEFAULTS)


def ensure_variables(defaults):
    for key, value in defaults.items():
        if Variable.get(key, default_var=None) is None:
            Variable.set(key, value)


def ensure_all_project_variables():
    ensure_variables(ALL_PROJECT_DEFAULTS)
