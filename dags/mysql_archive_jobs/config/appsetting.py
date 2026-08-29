import os

CONFIG_DIR = os.path.dirname(os.path.abspath(__file__))

# Slack
SLACK_CHANNEL = "your_slack_channel"
SLACK_TOKEN = "your_slack_channel_token"

# db_config account keys
POC_MYSQL_ACCOUNT = "POC_MySQL_account"
PROD_MYSQL_ACCOUNT = "PROD_MySQL_account"

# Config files under this package
ETL_TASK_RECORD_CSV = "etl_task_record.csv"
ETL_TASK_RECORD_SP_CSV = "etl_task_record_sp.csv"
CONVERT_RULE_JSON = "convert_rule.json"

# Control / log table names
MSSQL_ETL_TASK_TABLE = "etl_task_record_sp"
SHARDING_TABLE_LOG = "sharding_table_log"

def etl_task_table_name(airflow_task_id) -> str:
    return f"etl_task_record_{airflow_task_id}"


def etl_sync_ids_table_name(airflow_task_id) -> str:
    return f"etl_sync_ids_{airflow_task_id}"


# DAG ids (keep existing ids so Airflow history is unchanged)
MSSQL_TO_MYSQL_DAG_ID = "mssql_to_mysql_sync_tasks"
MYSQL_HOUSE_KEEPING_DAG_ID = "myssql_house_keeping_tasks"
MYSQL_TO_MYSQL_DAG_ID = "mysql_to_mysql_sync_tasks"

# Schedules are UTC+0
MSSQL_TO_MYSQL_SCHEDULE = "0 7 * * *"
MYSQL_HOUSE_KEEPING_SCHEDULE = "0 22 * * *"
MYSQL_TO_MYSQL_SCHEDULE = "0 17 * * *"

MSSQL_TO_MYSQL_TAGS = ["mssql", "mysql", "etl"]
MYSQL_HOUSE_KEEPING_TAGS = ["mysql", "etl", "Housekeeping"]
MYSQL_TO_MYSQL_TAGS = ["mysql", "sync", "etl"]

# MSSQL connection
MSSQL_ODBC_DRIVER = "ODBC Driver 17 for SQL Server"
MSSQL_CONNECT_TIMEOUT = 10
MSSQL_CONNECT_RETRIES = 3
MSSQL_CONNECT_RETRY_SLEEP_SECONDS = 30

# House keeping
HOUSE_KEEPING_MAX_RETRIES = 10
HOUSE_KEEPING_RETRY_SLEEP_SECONDS = 10
HOUSE_KEEPING_EXPIRED_DAYS = 14
HOUSE_KEEPING_SLEEP_BETWEEN_ROWS_SECONDS = 1

# Sharding log write retries
SHARDING_LOG_MAX_RETRIES = 5
SHARDING_LOG_RETRY_DELAY_SECONDS = 10

# Distribute / ETL retries
DISTRIBUTE_MAX_RETRIES = 10
DISTRIBUTE_RETRY_SLEEP_SECONDS = 10
ETL_LOOP_SLEEP_SECONDS = 2

# Timestamp validation used by sharding log updates
MIN_VALID_DATETIME_YEAR = 1990
INVALID_DATETIME_STR = "0001-01-01 00:00:00"
ETL_TYPE_3_LAG_DAYS = 3


def config_file(filename: str) -> str:
    return os.path.join(CONFIG_DIR, filename)


def mysql_account_key(is_poc: str) -> str:
    return POC_MYSQL_ACCOUNT if is_poc == "1" else PROD_MYSQL_ACCOUNT
