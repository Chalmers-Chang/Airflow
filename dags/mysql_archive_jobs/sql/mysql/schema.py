def show_source_table_sql(source_db: str, source_table: str) -> str:
    return f"""
            SHOW FULL TABLES IN `{source_db}`
            WHERE Tables_in_{source_db} = '{source_table}'
        """


def select_source_column_names_sql(source_db: str, source_table: str) -> str:
    return f"""
            SELECT COLUMN_NAME
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{source_db}'
            AND TABLE_NAME = '{source_table}';
        """


def select_source_columns_with_comments_sql(source_db: str, source_table: str) -> str:
    return f"""
            SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, COLUMN_COMMENT
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{source_db}'
            AND TABLE_NAME = '{source_table}';
        """


def select_source_column_types_sql(source_db: str, source_table: str) -> str:
    return f"""
            SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE, IS_NULLABLE, COLUMN_DEFAULT, EXTRA
            FROM INFORMATION_SCHEMA.COLUMNS
            WHERE TABLE_SCHEMA = '{source_db}'
            AND TABLE_NAME = '{source_table}';
        """


def select_table_comment_sql(source_db: str, source_table: str) -> str:
    return f"""
            SELECT TABLE_COMMENT
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{source_db}'
            AND TABLE_NAME = '{source_table}';
        """


def select_sharded_target_tables_sql(target_db: str, source_table: str) -> str:
    return f"""
            SELECT TABLE_NAME
            FROM INFORMATION_SCHEMA.TABLES
            WHERE TABLE_SCHEMA = '{target_db}'
            AND TABLE_NAME REGEXP '^{source_table}_[0-9]+$';
        """


def alter_table_comment_sql(target_db: str, target_table_name: str, table_comment: str) -> str:
    return f"""
                    ALTER TABLE `{target_db}`.`{target_table_name}`
                    COMMENT = '{table_comment}';
                """


def alter_column_comment_sql(
    target_db: str, target_table_name: str, column_name: str, column_type: str, column_comment: str
) -> str:
    return f"""
                        ALTER TABLE `{target_db}`.`{target_table_name}`
                        CHANGE COLUMN {column_name} {column_name} {column_type} COMMENT '{column_comment}';
                    """


def column_exists_sql(target_db: str, target_table_name: str, column_name: str) -> str:
    return f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.COLUMNS
                    WHERE TABLE_SCHEMA = '{target_db}'
                    AND TABLE_NAME = '{target_table_name}'
                    AND COLUMN_NAME = '{column_name}';
                """


def add_column_sql(
    target_db: str, target_table_name: str, column_name: str, column_definition: str
) -> str:
    return f"""
                        ALTER TABLE `{target_db}`.`{target_table_name}`
                        ADD COLUMN `{column_name}` {column_definition};
                    """


def select_column_datatypes_sql(source_db: str, source_table: str, fields_for_query: str) -> str:
    return f"""
        SELECT COLUMN_NAME, COLUMN_TYPE, CHARACTER_MAXIMUM_LENGTH, NUMERIC_PRECISION, NUMERIC_SCALE
        FROM INFORMATION_SCHEMA.COLUMNS
        WHERE TABLE_SCHEMA = '{source_db}'
        AND TABLE_NAME = '{source_table}'
        AND COLUMN_NAME IN ({fields_for_query});
    """


def create_sharded_table_sql(target_db: str, target_table: str, column_definitions_sql: str) -> str:
    return f"""
                        CREATE TABLE IF NOT EXISTS `{target_db}`.`{target_table}` ({column_definitions_sql});
                    """


def select_source_indexes_sql(source_db: str, source_table: str) -> str:
    return f"""
            SELECT INDEX_NAME, COLUMN_NAME, NON_UNIQUE, SEQ_IN_INDEX
            FROM INFORMATION_SCHEMA.STATISTICS
            WHERE TABLE_SCHEMA = '{source_db}'
            AND TABLE_NAME = '{source_table}'
            ORDER BY INDEX_NAME, SEQ_IN_INDEX;
        """


def index_exists_sql(target_db: str, target_table: str, index_name: str) -> str:
    return f"""
                    SELECT COUNT(*)
                    FROM INFORMATION_SCHEMA.STATISTICS
                    WHERE TABLE_SCHEMA = '{target_db}'
                    AND TABLE_NAME = '{target_table}'
                    AND INDEX_NAME = '{index_name}';
                """


def create_index_sql(unique: str, index_name: str, target_table_name: str, columns: str) -> str:
    return f"CREATE {unique} INDEX {index_name} ON {target_table_name} ({columns});"
