import general.toolbox as tb
from sql.common import table_exists_in_current_database_sql


def etl_task_record_sp_exists_sql(etl_task_table: str) -> str:
    return table_exists_in_current_database_sql(etl_task_table)


def create_etl_task_record_sp_sql(target_db: str, etl_task_table: str) -> str:
    return f"""
            CREATE TABLE `{target_db}`.`{etl_task_table}` (
                source_table_id int NOT NULL PRIMARY KEY,
                airflow_task_id int NOT NULL,
                is_active int DEFAULT TRUE,
                source_username VARCHAR(50) NOT NULL,
                source_password VARCHAR(150) NOT NULL,
                source_host VARCHAR(50) NOT NULL,
                source_db VARCHAR(50) NOT NULL,
                source_table VARCHAR(50) NOT NULL,
                source_sp VARCHAR(50) NOT NULL,
                etl_type int,
                arg_counts INT NOT NULL,
                target_username VARCHAR(50) NOT NULL,
                target_password VARCHAR(150) NOT NULL,
                target_host VARCHAR(50) NOT NULL,
                target_db VARCHAR(50) NOT NULL,
                target_table VARCHAR(50) NOT NULL,
                pk_field VARCHAR(50),
                sync_field longtext NOT NULL,
                max_loop_count INT NOT NULL,
                batch_size INT NOT NULL,
                id_field_name_1 VARCHAR(50),
                last_id_sync_record_1 BIGINT DEFAULT 0,
                id_field_name_2 VARCHAR(50),
                last_id_sync_record_2 BIGINT DEFAULT 0,
                timestamp_field_name VARCHAR(50),
                last_timestamp_sync_record VARBINARY(8)
                );"""


def upsert_etl_task_record_sp_sql(target_db: str, etl_task_table: str, row) -> str:
    v = tb.safe_sql_value
    return f"""
        INSERT INTO `{target_db}`.`{etl_task_table}`
        (source_table_id, airflow_task_id, is_active, source_username, source_password, source_host, source_db, source_table, source_sp, etl_type, arg_counts,
        target_username, target_password, target_host, target_db, target_table, pk_field, sync_field, max_loop_count,
        batch_size, id_field_name_1, last_id_sync_record_1, id_field_name_2, last_id_sync_record_2, timestamp_field_name,
        last_timestamp_sync_record)
        VALUES (
        {v(row['source_table_id'])}, {v(row['airflow_task_id'])}, {v(row['is_active'])},
        {v(row['source_username'])}, {v(row['source_password'])}, {v(row['source_host'])}, {v(row['source_db'])},
        {v(row['source_table'])}, {v(row['source_sp'])}, {v(row['etl_type'])}, {v(row['arg_counts'])},
        {v(row['target_username'])}, {v(row['target_password'])}, {v(row['target_host'])},
        {v(row['target_db'])}, {v(row['target_table'])}, {v(row['pk_field'])}, {v(row['sync_field'])},
        {v(row['max_loop_count'])}, {v(row['batch_size'])}, {v(row['id_field_name_1'])},
        {v(row['last_id_sync_record_1'])}, {v(row['id_field_name_2'])}, {v(row['last_id_sync_record_2'])}, {v(row['timestamp_field_name'])},
        {v(row['last_timestamp_sync_record'])})
        ON DUPLICATE KEY UPDATE
        airflow_task_id = {v(row['airflow_task_id'])},
        is_active = {v(row['is_active'])},
        source_username = {v(row['source_username'])},
        source_password = {v(row['source_password'])},
        source_host = {v(row['source_host'])},
        source_db = {v(row['source_db'])},
        source_table = {v(row['source_table'])},
        source_sp = {v(row['source_sp'])},
        etl_type = {v(row['etl_type'])},
        arg_counts = {v(row['arg_counts'])},
        target_username = {v(row['target_username'])},
        target_password = {v(row['target_password'])},
        target_host = {v(row['target_host'])},
        target_db = {v(row['target_db'])},
        target_table = {v(row['target_table'])},
        pk_field = {v(row['pk_field'])},
        max_loop_count = {v(row['max_loop_count'])},
        batch_size = {v(row['batch_size'])},
        id_field_name_1 = {v(row['id_field_name_1'])},
        id_field_name_2 = {v(row['id_field_name_2'])},
        timestamp_field_name = {v(row['timestamp_field_name'])};
        """


def select_active_mssql_tasks_sql(target_db: str, etl_task_table: str, airflow_task_id) -> str:
    return (
        f"SELECT * FROM `{target_db}`.`{etl_task_table}` "
        f"WHERE is_active = 1 and  airflow_task_id = {airflow_task_id} "
    )


def update_watermark_sql(
    target_db: str,
    etl_task_table: str,
    target_field: str,
    update_value,
    source_table_id,
) -> str:
    return f"""
                update `{target_db}`.`{etl_task_table}`
                set {target_field} = {update_value}
                where source_table_id = {source_table_id};
            """
