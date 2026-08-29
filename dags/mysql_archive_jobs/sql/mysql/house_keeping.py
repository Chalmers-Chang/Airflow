def delete_expired_sync_ids_sql(schema: str, etl_sync_ids: str, expired_days: int) -> str:
    return f"""
                delete from {schema}.{etl_sync_ids}
                where 1=1
                and is_source_delete = 1
                and date(etl_update_date) < DATE_sub(CURRENT_DATE(), interval {expired_days} day );
                """


def select_pending_sync_ids_sql(
    schema: str,
    etl_sync_ids: str,
    date_sub_start_day,
    date_sub_end_day,
    source_db: str,
    source_table: str,
) -> str:
    return f"""
                select house_keeping_id, ids_compress
                from {schema}.{etl_sync_ids}
                where 1=1
                and date(etl_update_date) >= DATE_sub(CURRENT_DATE(), interval {date_sub_start_day} day )
                and date(etl_update_date) < DATE_sub(CURRENT_DATE(), interval {date_sub_end_day} day )
                and source_db = '{source_db}'
                and source_table = '{source_table}'
                and is_source_delete = 0
                """


def select_recent_source_ids_sql(
    source_db: str,
    source_table: str,
    timestamp_field_name: str,
    id_field_name: str,
    synced_id_list: str,
    expired_days: int,
) -> str:
    return f"""select {timestamp_field_name}
                FROM {source_db}.{source_table}
                WHERE {id_field_name} IN ({synced_id_list})
                and date({timestamp_field_name}) >= DATE_sub(CURRENT_DATE(), interval {expired_days} day );
             """


def delete_source_by_ids_sql(
    source_db: str, source_table: str, id_field_name: str, synced_id_list: str
) -> str:
    return f"""DELETE FROM {source_db}.{source_table}
                WHERE {id_field_name} IN ({synced_id_list});
             """


def mark_sync_ids_deleted_sql(schema: str, etl_sync_ids: str, house_keeping_id) -> str:
    return f"""UPDATE {schema}.{etl_sync_ids}
                SET is_source_delete = 1
                WHERE house_keeping_id = {house_keeping_id};
             """


def optimize_table_sql(source_db: str, source_table: str) -> str:
    return f"OPTIMIZE TABLE {source_db}.{source_table};"
