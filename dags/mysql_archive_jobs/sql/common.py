def table_exists_sql(schema: str, table_name: str) -> str:
    return f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = '{schema}'
        AND table_name = '{table_name}';
    """


def table_exists_in_current_database_sql(table_name: str) -> str:
    return f"""
        SELECT COUNT(*)
        FROM information_schema.tables
        WHERE table_schema = DATABASE()
        AND table_name = '{table_name}';
    """
