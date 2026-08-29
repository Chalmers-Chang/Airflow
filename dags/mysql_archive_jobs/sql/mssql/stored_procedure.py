def generate_query_sql_statement_by_config(config) -> tuple:
    if config.etl_type == 1:
        if config.arg_counts == 2:
            sql = (
                f"EXEC dbo.{config.source_sp} @batch_Count=?, "
                f"@min_{config.id_field_name_1}=?"
            )
            return sql, (config.batch_size, config.last_id_sync_record_1)
        if config.arg_counts == 3:
            sql = (
                f"EXEC dbo.{config.source_sp} @batch_Count=?, "
                f"@min_{config.id_field_name_1}=?, "
                f"@min_{config.id_field_name_2}=?"
            )
            return sql, (
                config.batch_size,
                config.last_id_sync_record_1,
                config.last_id_sync_record_2,
            )

    elif config.etl_type == 2:
        if config.arg_counts == 2:
            min_timestamp_value = config.last_timestamp_sync_record.to_bytes(8, byteorder="big")
            sql = f"EXEC dbo.{config.source_sp} @batch_Count=?, @min_SN=?"
            return sql, (config.batch_size, min_timestamp_value)

    elif config.etl_type == 4:
        if config.arg_counts == 2:
            sql = (
                f"EXEC dbo.{config.source_sp} @batch_Count=?, "
                f"@min_{config.id_field_name_1}=?"
            )
            return sql, (config.batch_size, config.last_id_sync_record_1)
        if config.arg_counts == 3:
            sql = (
                f"EXEC dbo.{config.source_sp} @batch_Count=?, "
                f"@min_{config.id_field_name_1}=?, "
                f"@min_{config.id_field_name_2}=?"
            )
            return sql, (
                config.batch_size,
                config.last_id_sync_record_1,
                config.last_id_sync_record_2,
            )

    raise ValueError("Invalid number of etl_type & arguments in config.")
