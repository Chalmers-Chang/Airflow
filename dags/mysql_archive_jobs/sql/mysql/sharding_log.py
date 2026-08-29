def create_sharding_table_log_sql(target_db: str, sharding_table_log: str) -> str:
    return f"""
            CREATE TABLE if not exists {target_db}.{sharding_table_log} (
                sharding_table_name VARCHAR(255) NOT NULL PRIMARY KEY,
                shard_id int ,
                source_db_name VARCHAR(255) NOT NULL,
                source_table_name VARCHAR(255) NOT NULL,
                id_field_name VARCHAR(50),
                min_id BIGINT DEFAULT 0,
                max_id BIGINT DEFAULT 0 ,
                matchdate_field_name VARCHAR(255),
                min_matchdate DATETIME ,
                max_matchdate DATETIME ,
                timestamp_field_name VARCHAR(255),
                min_timestamp TIMESTAMP ,
                max_timestamp TIMESTAMP ,
                latest_update_time TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
                current_columns longtext NOT NULL
            );
        """


def upsert_sharding_log_sql(target_db: str, sharding_table_log: str) -> str:
    return f"""
                INSERT INTO {target_db}.{sharding_table_log}
                    (sharding_table_name, shard_id, source_db_name, source_table_name, id_field_name, matchdate_field_name, timestamp_field_name, latest_update_time, current_columns)
                VALUES
                    (%s, %s, %s, %s, %s, %s, %s, NOW(), %s)
                ON DUPLICATE KEY UPDATE
                    source_db_name = VALUES(source_db_name),
                    source_table_name = VALUES(source_table_name),
                    id_field_name = VALUES(id_field_name),
                    matchdate_field_name = VALUES(matchdate_field_name),
                    timestamp_field_name = VALUES(timestamp_field_name),
                    latest_update_time = NOW(),
                    current_columns = VALUES(current_columns);
            """


def update_min_id_sql(target_db: str, sharding_table_log: str, log_object) -> str:
    return f"""
                    UPDATE {target_db}.{sharding_table_log}
                    SET min_id = {log_object.min_id}
                    WHERE sharding_table_name = '{log_object.sharding_table_name}'
                    AND ({log_object.min_id} < min_id or min_id = 0);
                """


def update_max_id_sql(target_db: str, sharding_table_log: str, log_object) -> str:
    return f"""
                    UPDATE {target_db}.{sharding_table_log}
                    SET max_id = {log_object.max_id}
                    WHERE sharding_table_name = '{log_object.sharding_table_name}'
                    AND {log_object.max_id} > max_id;
                """


def update_min_matchdate_sql(target_db: str, sharding_table_log: str, log_object) -> str:
    return f"""
                    UPDATE {target_db}.{sharding_table_log}
                    SET min_matchdate = '{log_object.min_matchdate}'
                    WHERE sharding_table_name = '{log_object.sharding_table_name}'
                    AND ('{log_object.min_matchdate}' < min_matchdate or min_matchdate is null);
                """


def update_max_matchdate_sql(target_db: str, sharding_table_log: str, log_object) -> str:
    return f"""
                    UPDATE {target_db}.{sharding_table_log}
                    SET max_matchdate = '{log_object.max_matchdate}'
                    WHERE sharding_table_name = '{log_object.sharding_table_name}'
                    AND ('{log_object.max_matchdate}' > max_matchdate or max_matchdate is null);
                """


def update_min_timestamp_sql(target_db: str, sharding_table_log: str) -> str:
    return f"""
                        UPDATE {target_db}.{sharding_table_log}
                        SET min_timestamp = %s
                        WHERE sharding_table_name = %s
                        AND (%s < min_timestamp OR min_timestamp IS NULL);
                    """


def update_max_timestamp_sql(target_db: str, sharding_table_log: str) -> str:
    return f"""
                        UPDATE {target_db}.{sharding_table_log}
                        SET max_timestamp = %s
                        WHERE sharding_table_name = %s
                        AND (%s > max_timestamp OR max_timestamp IS NULL);
                    """
