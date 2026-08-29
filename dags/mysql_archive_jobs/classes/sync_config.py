import time

import pyodbc
import pymysql

import general.toolbox as tb
from config import appsetting


class SyncConfig:
    def __init__(
        self,
        source_table_id: int,
        airflow_task_id: int,
        is_active: bool,
        source_username: str,
        source_password: str,
        source_host: str,
        source_db: str,
        source_table: str,
        source_sp: str,
        etl_type: int,
        arg_counts: int,
        target_username: str,
        target_password: str,
        target_host: str,
        target_db: str,
        target_table: str,
        pk_field: str,
        sync_field: str,
        max_loop_count: int,
        batch_size: int,
        id_field_name_1: str,
        last_id_sync_record_1: int,
        id_field_name_2: str,
        last_id_sync_record_2: int,
        timestamp_field_name: str,
        last_timestamp_sync_record: str,
        crypto_key: str,
    ):
        self.source_table_id = source_table_id
        self.airflow_task_id = airflow_task_id
        self.is_active = bool(is_active)
        self.source_username = source_username
        self.source_password = tb.password_decode(crypto_key, source_password)
        self.source_host = source_host
        self.source_db = source_db
        self.source_table = source_table
        self.source_sp = source_sp
        self.etl_type = etl_type
        self.arg_counts = arg_counts
        self.target_username = target_username
        self.target_password = tb.password_decode(crypto_key, target_password)
        self.target_host = target_host
        self.target_db = target_db
        self.target_table = target_table
        self.pk_field = pk_field
        self.sync_field = sync_field
        self.max_loop_count = max_loop_count
        self.batch_size = batch_size
        self.id_field_name_1 = id_field_name_1
        self.last_id_sync_record_1 = last_id_sync_record_1
        self.id_field_name_2 = id_field_name_2
        self.last_id_sync_record_2 = last_id_sync_record_2
        self.timestamp_field_name = timestamp_field_name
        self.last_timestamp_sync_record = last_timestamp_sync_record

    def mssql_start_source_engine(self) -> pyodbc.Connection:
        for attempt in range(appsetting.MSSQL_CONNECT_RETRIES):
            try:
                if attempt > 0:
                    print(
                        f"Attempt {attempt}/{appsetting.MSSQL_CONNECT_RETRIES}: "
                        f"Connecting to MSSQL {self.source_host} as {self.source_username}..."
                    )
                self.source_connection = pyodbc.connect(
                    f"DRIVER={{{appsetting.MSSQL_ODBC_DRIVER}}};"
                    f"SERVER={self.source_host};"
                    f"DATABASE={self.source_db};"
                    f"UID={self.source_username};"
                    f"PWD={self.source_password}",
                    timeout=appsetting.MSSQL_CONNECT_TIMEOUT,
                )
                return self.source_connection
            except pyodbc.Error as e:
                print(
                    f"MSSQL connection failed "
                    f"(Attempt {attempt + 1}/{appsetting.MSSQL_CONNECT_RETRIES}): {e}"
                )
                self.source_connection = None
                time.sleep(appsetting.MSSQL_CONNECT_RETRY_SLEEP_SECONDS)

        print("All MSSQL connection attempts failed.")
        return None

    def mssql_dispose_source_engine(self) -> None:
        if hasattr(self, "source_connection") and self.source_connection:
            try:
                self.source_connection.close()
            except Exception as e:
                print(f"MSSQL connection failed to close: {e}")
            finally:
                self.source_connection = None

    def mysql_start_target_engine(self) -> pymysql.Connection:
        try:
            self.target_connection = pymysql.connect(
                host=self.target_host,
                user=self.target_username,
                password=self.target_password,
                database=self.target_db,
            )
            return self.target_connection
        except Exception as e:
            print(f"MySQL connection failed to close: {e}")
            self.target_connection = None
            return None

    def mysql_dispose_target_engine(self) -> None:
        if self.target_connection:
            try:
                self.target_connection.close()
            except Exception as e:
                print(f"MySQL connection failed to close: {e}")
            finally:
                self.target_connection = None
