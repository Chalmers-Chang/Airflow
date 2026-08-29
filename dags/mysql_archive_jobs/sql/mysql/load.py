def upsert_sharded_row_sql(
    target_db: str, target_table: str, column_names: str, placeholder_names: str, update_clause: str
) -> str:
    return f"""
                INSERT INTO `{target_db}`.`{target_table}` ({column_names})
                VALUES ({placeholder_names})
                ON DUPLICATE KEY UPDATE {update_clause}
            """


def truncate_table_sql(target_db: str, target_table: str) -> str:
    return f"TRUNCATE TABLE `{target_db}`.`{target_table}`;"


def insert_row_sql(target_db: str, target_table: str, column_names: str, placeholder_names: str) -> str:
    return f"""
            INSERT INTO `{target_db}`.`{target_table}` ({column_names})
            VALUES ({placeholder_names})
            """
