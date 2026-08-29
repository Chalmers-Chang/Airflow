from config import appsetting


def formatted_sync_fields(sync_field: str) -> str:
    return ", ".join([f"`{field.strip()}`" for field in sync_field.split(",")])


def partition_num_expr(id_field_name, partition_size) -> str:
    return f"""CASE
                    WHEN {id_field_name} REGEXP '^[0-9]+$'
                    THEN {id_field_name}
                    ELSE CRC32({id_field_name})
                END DIV {partition_size} + 1"""


def generate_query_sql(db_connection_config, timestamp_tag) -> str:
    if timestamp_tag.datetime_str is None:
        last_match_datetime_str = "1970-01-01 0:00:01"

    formatted = formatted_sync_fields(db_connection_config.sync_field)

    if isinstance(db_connection_config.last_id_sync_record, str):
        db_connection_config.last_id_sync_record = 0

    if db_connection_config.etl_type == 1:
        return f"""
            SELECT {formatted},
                {partition_num_expr(db_connection_config.id_field_name, db_connection_config.partition_size)} AS partition_num
            FROM `{db_connection_config.source_db}`.`{db_connection_config.source_table}`
            WHERE {db_connection_config.id_field_name} > {db_connection_config.last_id_sync_record}
            ORDER BY {db_connection_config.id_field_name} asc
            LIMIT {db_connection_config.batch_size}
        """

    if db_connection_config.etl_type == 2:
        last_timestamp_sync_record = db_connection_config.last_timestamp_sync_record.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        generated_sql = f"""
            SELECT {formatted},
                {partition_num_expr(db_connection_config.id_field_name, db_connection_config.partition_size)} AS partition_num
            FROM `{db_connection_config.source_db}`.`{db_connection_config.source_table}`
            WHERE ({db_connection_config.timestamp_field_name} > '{last_timestamp_sync_record}'
            OR ({db_connection_config.timestamp_field_name} = '{last_timestamp_sync_record}' )
            AND {db_connection_config.id_field_name} > {db_connection_config.last_id_sync_record})
            ORDER BY {db_connection_config.timestamp_field_name}, {db_connection_config.id_field_name}
            LIMIT {db_connection_config.batch_size}
        """
        print(
            f"Set timestamp = '{last_timestamp_sync_record}', "
            f"last_id_sync_record =  {db_connection_config.last_id_sync_record}"
        )
        return generated_sql

    if db_connection_config.etl_type == 3:
        last_timestamp_sync_record_str = db_connection_config.last_timestamp_sync_record.strftime(
            "%Y-%m-%d %H:%M:%S"
        )
        last_match_datetime_str = timestamp_tag.datetime_str
        if last_timestamp_sync_record_str != last_match_datetime_str:
            db_connection_config.last_id_sync_record = 0

        generated_sql = f"""
            SELECT {formatted},
                {partition_num_expr(db_connection_config.id_field_name, db_connection_config.partition_size)} AS partition_num
            FROM `{db_connection_config.source_db}`.`{db_connection_config.source_table}`
            WHERE
                ({db_connection_config.timestamp_field_name} >= '{last_timestamp_sync_record_str}'
                AND {db_connection_config.id_field_name} > {db_connection_config.last_id_sync_record})
                AND {db_connection_config.timestamp_field_name} < DATE(DATE_SUB(NOW(), INTERVAL {appsetting.ETL_TYPE_3_LAG_DAYS} DAY))
            ORDER BY  {db_connection_config.timestamp_field_name} asc , {db_connection_config.id_field_name} asc
            LIMIT {db_connection_config.batch_size}
        """
        timestamp_tag.datetime_str = last_timestamp_sync_record_str
        return generated_sql

    if db_connection_config.etl_type == 4:
        return f"""
            SELECT {formatted}, 0 as partition_num
            FROM `{db_connection_config.source_db}`.`{db_connection_config.source_table}`
        """

    return "SELECT 0 WHERE 0 > 1"
