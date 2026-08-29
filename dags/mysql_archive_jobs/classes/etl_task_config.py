import pandas as pd
import pymysql

import general.toolbox as tb


class etl_task_connection_config:
    def __init__(
        self,
        source_username,
        source_password,
        source_host,
        source_db,
        source_table,
        target_username,
        target_password,
        target_host,
        target_db,
        target_table,
        pk_field,
        sync_field,
        etl_type,
        max_loop_count,
        batch_size,
        partition_size,
        id_field_name,
        last_id_sync_record,
        last_id_house_keeping_record,
        timestamp_field_name,
        last_timestamp_sync_record,
        last_timestamp_house_keeping_record,
        source_write_username,
        source_write_password,
        if_need_house_keeping,
        crypto_key,
        event_date_field_name=None,
    ):
        self.source_username = source_username
        self.source_password = tb.password_decode(crypto_key, source_password)
        self.source_host = source_host
        self.source_db = source_db
        self.source_table = source_table
        self.source_engine = None
        self.target_username = target_username
        self.target_password = tb.password_decode(crypto_key, target_password)
        self.target_host = target_host
        self.target_db = target_db
        self.target_table = target_table
        self.target_engine = None
        self.pk_field = pk_field
        self.sync_field = sync_field
        self.etl_type = etl_type
        self.max_loop_count = max_loop_count
        self.batch_size = batch_size
        self.partition_size = partition_size
        self.id_field_name = id_field_name
        self.last_id_sync_record = last_id_sync_record
        self.last_id_house_keeping_record = last_id_house_keeping_record
        self.timestamp_field_name = timestamp_field_name
        self.last_timestamp_sync_record = last_timestamp_sync_record
        self.last_timestamp_house_keeping_record = last_timestamp_house_keeping_record
        self.source_write_username = source_write_username
        self.source_write_password = tb.password_decode(crypto_key, source_write_password)
        self.if_need_house_keeping = if_need_house_keeping
        self.event_date_field_name = event_date_field_name

    def start_source_engine(self):
        self.source_connection = pymysql.connect(
            host=self.source_host,
            user=self.source_username,
            password=self.source_password,
            database=self.source_db,
        )
        return self.source_connection

    def dispose_source_engine(self):
        if self.source_connection:
            self.source_connection.close()

    def start_source_house_keeping_engine(self):
        self.source_connection = pymysql.connect(
            host=self.source_host,
            user=self.source_write_username,
            password=self.source_write_password,
            database=self.source_db,
        )
        return self.source_connection

    def dispose_source_house_keeping_engine(self):
        if self.source_connection:
            self.source_connection.close()

    def start_target_engine(self):
        self.target_connection = pymysql.connect(
            host=self.target_host,
            user=self.target_username,
            password=self.target_password,
            database=self.target_db,
        )
        return self.target_connection

    def dispose_target_engine(self):
        if self.target_connection:
            self.target_connection.close()

    @classmethod
    def from_target_db_config(cls, target_db_config, crypto_key):
        return cls(
            source_username=None,
            source_password=None,
            source_host=None,
            source_db=None,
            source_table=None,
            target_username=target_db_config.username,
            target_password=target_db_config.password,
            target_host=target_db_config.host,
            target_db=target_db_config.db,
            target_table=None,
            pk_field=None,
            sync_field=None,
            etl_type=None,
            max_loop_count=None,
            batch_size=None,
            partition_size=None,
            id_field_name=None,
            last_id_sync_record=None,
            last_id_house_keeping_record=None,
            timestamp_field_name=None,
            last_timestamp_sync_record=None,
            last_timestamp_house_keeping_record=None,
            source_write_username=None,
            source_write_password=None,
            if_need_house_keeping=None,
            crypto_key=crypto_key,
            event_date_field_name=None,
        )


def create_etl_task_connection_config(row, crypto_key):
    event_date_field_name = None
    if "event_date_field_name" in getattr(row, "index", []):
        event_date_field_name = (
            None if pd.isna(row["event_date_field_name"]) else row["event_date_field_name"]
        )

    return etl_task_connection_config(
        source_username=row["source_username"],
        source_password=row["source_password"],
        source_host=row["source_host"],
        source_db=row["source_db"],
        source_table=row["source_table"],
        target_username=row["target_username"],
        target_password=row["target_password"],
        target_host=row["target_host"],
        target_db=row["target_db"],
        target_table=row["target_table"],
        pk_field=row["pk_field"],
        sync_field=row["sync_field"],
        etl_type=row["etl_type"],
        max_loop_count=row["max_loop_count"],
        batch_size=row["batch_size"],
        partition_size=row["partition_size"],
        id_field_name=None if pd.isna(row["id_field_name"]) else row["id_field_name"],
        last_id_sync_record=row["last_id_sync_record"],
        last_id_house_keeping_record=row["last_id_house_keeping_record"],
        timestamp_field_name=(
            None if pd.isna(row["timestamp_field_name"]) else row["timestamp_field_name"]
        ),
        last_timestamp_sync_record=row["last_timestamp_sync_record"],
        last_timestamp_house_keeping_record=row["last_timestamp_house_keeping_record"],
        source_write_username=row["source_write_username"],
        source_write_password=row["source_write_password"],
        if_need_house_keeping=row["if_need_house_keeping"],
        crypto_key=crypto_key,
        event_date_field_name=event_date_field_name,
    )
