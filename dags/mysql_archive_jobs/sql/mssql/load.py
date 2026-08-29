def upsert_row_sql(target_db: str, target_table: str, column_names) -> str:
    columns = ", ".join([f"`{col}`" for col in column_names])
    placeholders = ", ".join(["%s" if col != "inserttime" else "NOW()" for col in column_names])
    update_clause = ", ".join([f"`{col}`=VALUES(`{col}`)" for col in column_names])
    return f"""
                    INSERT INTO `{target_db}`.`{target_table}` ({columns})
                    VALUES ({placeholders})
                    ON DUPLICATE KEY UPDATE {update_clause}
                """
