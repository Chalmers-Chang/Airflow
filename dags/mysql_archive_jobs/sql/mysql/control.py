from sql.common import table_exists_sql


def etl_task_record_exists_sql(schema: str, etl_task_table: str) -> str:
    return table_exists_sql(schema, etl_task_table)


def etl_sync_ids_exists_sql(schema: str, etl_sync_ids: str) -> str:
    return table_exists_sql(schema, etl_sync_ids)


def create_etl_task_record_sql(schema: str, etl_task_table: str) -> str:
    return f"""
                CREATE TABLE `{schema}`.`{etl_task_table}` (
                    source_table_id int NOT NULL PRIMARY KEY,
                    airflow_task_id int NOT NULL,
                    is_active int DEFAULT TRUE,
                    source_username VARCHAR(50) NOT NULL,
                    source_password VARCHAR(150) NOT NULL,
                    source_host VARCHAR(50) NOT NULL,
                    source_db VARCHAR(50) NOT NULL,
                    source_table VARCHAR(50) NOT NULL,
                    target_username VARCHAR(50) NOT NULL,
                    target_password VARCHAR(150) NOT NULL,
                    target_host VARCHAR(50) NOT NULL,
                    target_db VARCHAR(50) NOT NULL,
                    target_table VARCHAR(50) NOT NULL,
                    pk_field VARCHAR(50),
                    sync_field longtext NOT NULL,
                    etl_type INT NOT NULL,
                    max_loop_count INT NOT NULL,
                    batch_size INT NOT NULL,
                    partition_size BIGINT NOT NULL,
                    id_field_name VARCHAR(50),
                    last_id_sync_record BIGINT DEFAULT 0,
                    last_id_house_keeping_record BIGINT DEFAULT 0,
                    timestamp_field_name VARCHAR(50),
                    last_timestamp_sync_record datetime DEFAULT '1970-01-01 00:00:01',
                    last_timestamp_house_keeping_record datetime DEFAULT '1970-01-01 00:00:01',
                    source_write_username VARCHAR(50) NOT NULL,
                    source_write_password VARCHAR(150) NOT NULL,
                    if_need_house_keeping INT DEFAULT 0,
                    event_date_field_name VARCHAR(50)
                    );"""


def create_etl_sync_ids_sql(schema: str, etl_sync_ids: str) -> str:
    return f"""
                    CREATE TABLE `{schema}`.`{etl_sync_ids}` (
                        house_keeping_id INT NOT NULL AUTO_INCREMENT,
                        etl_update_date DATETIME NOT NULL,
                        source_db VARCHAR(50) NOT NULL,
                        source_table VARCHAR(50) NOT NULL,
                        ids_compress BLOB NOT NULL COMMENT 'plz use python zlib to decompress',
                        is_source_delete tinyint(1) DEFAULT 0,
                        PRIMARY KEY (house_keeping_id)
                    );"""


def upsert_etl_task_record_sql(schema: str, etl_task_table: str, row) -> str:
    return f"""
            INSERT INTO `{schema}`.`{etl_task_table}`
            (source_table_id, airflow_task_id, is_active, source_username, source_password, source_host, source_db, source_table,
            target_username, target_password, target_host, target_db, target_table, pk_field, sync_field, etl_type, max_loop_count,
            batch_size, partition_size, id_field_name, last_id_sync_record, last_id_house_keeping_record, timestamp_field_name,
            last_timestamp_sync_record, last_timestamp_house_keeping_record, source_write_username, source_write_password, if_need_house_keeping, event_date_field_name)
            VALUES (
            {row['source_table_id']}, {row['airflow_task_id']}, {row['is_active']},
            '{row['source_username']}', '{row['source_password']}', '{row['source_host']}', '{row['source_db']}',
            '{row['source_table']}', '{row['target_username']}', '{row['target_password']}', '{row['target_host']}',
            '{row['target_db']}', '{row['target_table']}', '{row['pk_field']}', '{row['sync_field']}', {row['etl_type']},
            {row['max_loop_count']}, {row['batch_size']}, {row['partition_size']}, '{row['id_field_name']}',
            {row['last_id_sync_record']}, {row['last_id_house_keeping_record']}, '{row['timestamp_field_name']}',
            '{row['last_timestamp_sync_record']}', '{row['last_timestamp_house_keeping_record']}', '{row['source_write_username']}', '{row['source_write_password']}', '{row['if_need_house_keeping']}', '{row['event_date_field_name']}')
            ON DUPLICATE KEY UPDATE
            airflow_task_id = {row['airflow_task_id']},
            is_active = {row['is_active']},
            source_username = '{row['source_username']}',
            source_password = '{row['source_password']}',
            source_host = '{row['source_host']}',
            source_db = '{row['source_db']}',
            source_table = '{row['source_table']}',
            target_username = '{row['target_username']}',
            target_password = '{row['target_password']}',
            target_host = '{row['target_host']}',
            target_db = '{row['target_db']}',
            target_table = '{row['target_table']}',
            pk_field = '{row['pk_field']}',
            etl_type = {row['etl_type']},
            max_loop_count = {row['max_loop_count']},
            batch_size = {row['batch_size']},
            partition_size = {row['partition_size']},
            id_field_name = '{row['id_field_name']}',
            timestamp_field_name = '{row['timestamp_field_name']}',
            source_write_username = '{row['source_write_username']}',
            source_write_password = '{row['source_write_password']}',
            if_need_house_keeping = '{row['if_need_house_keeping']}',
            event_date_field_name = '{row['event_date_field_name']}'
            ;"""


def select_active_sync_tasks_sql(schema: str, etl_task_table: str, airflow_task_id) -> str:
    return (
        f"SELECT * FROM `{schema}`.`{etl_task_table}` "
        f"WHERE is_active = 1 and  airflow_task_id = {airflow_task_id} and etl_type != 0"
    )


def select_house_keeping_tasks_sql(schema: str, etl_task_table: str, airflow_task_id) -> str:
    return f"""SELECT * FROM `{schema}`.`{etl_task_table}`
        WHERE is_active = 1 and  airflow_task_id = {airflow_task_id} and etl_type not in (0,4) and if_need_house_keeping = 1
        """


def select_sync_field_sql(schema: str, etl_task_table: str, source_db: str, source_table: str) -> str:
    return f"""
            SELECT sync_field
            FROM `{schema}`.`{etl_task_table}`
            WHERE source_db = '{source_db}'
            AND source_table = '{source_table}';
        """


def update_sync_field_sql(
    schema: str, etl_task_table: str, column_names: str, source_db: str, source_table: str
) -> str:
    return f"""
            UPDATE `{schema}`.`{etl_task_table}`
            SET sync_field = '{column_names}'
            WHERE source_db = '{source_db}'
            AND source_table = '{source_table}';
        """


def update_watermark_sql(
    schema: str,
    etl_task_table: str,
    set_column: str,
    set_value,
    target_db: str,
    target_table: str,
) -> str:
    return f"""
        UPDATE `{schema}`.`{etl_task_table}`
        SET `{set_column}` = {set_value}
        WHERE target_db = '{target_db}' AND target_table = '{target_table}'
    """


def insert_compressed_ids_sql(schema: str, etl_sync_ids: str) -> str:
    return f"""
        INSERT INTO `{schema}`.`{etl_sync_ids}`
        (etl_update_date, source_db, source_table, ids_compress, is_source_delete)
        VALUES (CURRENT_DATE(), %s, %s, %s, 0)
    """
