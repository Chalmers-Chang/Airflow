import json
import time
from datetime import datetime

import numpy as np
import pandas as pd
import pymysql
import zlib
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from classes.etl_task_config import create_etl_task_connection_config, etl_task_connection_config
from classes.sharding import (
    gen_empty_sharding_table_log_opject,
    timestamp_tag,
)
from config import appsetting
from config.db_config import db_config
from slack.notifier import init_slack, send_msg_to_multiple_slack_channel
from sql.mysql.control import (
    create_etl_sync_ids_sql,
    create_etl_task_record_sql,
    etl_sync_ids_exists_sql,
    etl_task_record_exists_sql,
    insert_compressed_ids_sql,
    select_active_sync_tasks_sql,
    select_sync_field_sql,
    update_sync_field_sql,
    update_watermark_sql,
    upsert_etl_task_record_sql,
)
from sql.mysql.load import insert_row_sql, truncate_table_sql, upsert_sharded_row_sql
from sql.mysql.schema import (
    add_column_sql,
    alter_column_comment_sql,
    alter_table_comment_sql,
    column_exists_sql,
    create_index_sql,
    create_sharded_table_sql,
    index_exists_sql,
    select_column_datatypes_sql,
    select_sharded_target_tables_sql,
    select_source_column_names_sql,
    select_source_column_types_sql,
    select_source_columns_with_comments_sql,
    select_source_indexes_sql,
    select_table_comment_sql,
    show_source_table_sql,
)
from sql.mysql.sharding_log import (
    create_sharding_table_log_sql,
    update_max_id_sql,
    update_max_matchdate_sql,
    update_max_timestamp_sql,
    update_min_id_sql,
    update_min_matchdate_sql,
    update_min_timestamp_sql,
    upsert_sharding_log_sql,
)
from sql.mysql.sync_query import generate_query_sql

# Chalmers 2024-12-12 18:07 commit


def starting_stage():
    
    if is_poc:
        etl_start_msg = f'============= (ignore) start testing etl_tasks based on {etl_task_table} ============='
        send_msg_to_multiple_slack_channel(etl_start_msg)   
    else:
        etl_start_msg = f'============= start running etl_tasks based on {etl_task_table} ============='
        send_msg_to_multiple_slack_channel(etl_start_msg)   


def check_and_update_etl_task_record(etl_task_table, db_connection_config, is_config_updated):

    if is_config_updated == '1':

        # create target connection
        target_connection = db_connection_config.start_target_engine()
        
        # prepare task table
        check_task_table_sql = etl_task_record_exists_sql(target_db_config.db, etl_task_table)

        check_id_table_sql = etl_sync_ids_exists_sql(target_db_config.db, etl_sync_ids)
            
        # open target cursor
        with target_connection.cursor() as cursor: 
            
            cursor.execute(check_task_table_sql)
            result_task = cursor.fetchone()[0]

            cursor.execute(check_id_table_sql)
            result_id = cursor.fetchone()[0]        
            
            # if task table not exist, create one by airflow_task_id
            if result_task == 0:
                create_task_table_sql = create_etl_task_record_sql(target_db_config.db, etl_task_table)

                with target_connection.cursor() as cursor:  
                    cursor.execute(create_task_table_sql)
                    target_connection.commit()

            # if ids table not exist, create one by airflow_task_id
            if result_id == 0:
                create_id_table_sql = create_etl_sync_ids_sql(
                    db_connection_config.target_db, etl_sync_ids
                )
                
                # close target cursor
                with target_connection.cursor() as cursor:  
                    cursor.execute(create_id_table_sql)
                    target_connection.commit()

        # declare variables
        csv_file_path = appsetting.config_file(appsetting.ETL_TASK_RECORD_CSV)
        etl_task_record_df = pd.read_csv(csv_file_path, index_col=False)    

        # use task_csv insert task 
        for _, row in etl_task_record_df.iterrows():
            insert_sql = upsert_etl_task_record_sql(target_db_config.db, etl_task_table, row)
            
            # close target cursor
            with target_connection.cursor() as cursor:  
                cursor.execute(insert_sql)
                target_connection.commit()

        # close target connection
        db_connection_config.dispose_target_engine()

        Variable.set("is_config_updated","0")



def check_table_exists(db_connection_config):

    try:
        # create source connection
        source_engine = db_connection_config.start_source_engine()
        # prepare sql query to check if table exists
        query = show_source_table_sql(db_connection_config.source_db, db_connection_config.source_table)
        # open source cursor
        with source_engine.cursor() as cursor:
            cursor.execute(query)
            result = cursor.fetchone()
        # return next task_id as python operator
        if result:
            print(f"Table exists: {result}")
            return f"{db_connection_config.etl_type}_{db_connection_config.source_db}_{db_connection_config.source_table}_process"
        else:
            print(f"No table found for {db_connection_config.source_table}")
            return f"{db_connection_config.etl_type}_{db_connection_config.source_db}_{db_connection_config.source_table}_break_stage"

    except Exception as e:
        error_message = f"An error occurred while checking if the table exists: {str(e)}"
        print(error_message)
        raise

def check_sharding_table_log_exists(db_connection_config):

    target_connection = db_connection_config.start_target_engine() 

    try:
        # Update the sync_field in the etl_task_table
        create_table_query = create_sharding_table_log_sql(
            db_connection_config.target_db, sharding_table_log
        )

        # Execute the update query
        with target_connection.cursor() as cursor:
            cursor.execute(create_table_query)
            target_connection.commit()

        print(f"CREATE sharding_table_log in {db_connection_config.target_db} successfully.")
        return True

    except Exception as e:
        print(f"Error creating sharding_table_log: {e}")
    finally:
        db_connection_config.dispose_target_engine()

    return False

def insert_log_data(target_connection, db_connection_config, sharding_table_log_opject):
    
    attempt = 0
    max_retries = appsetting.SHARDING_LOG_MAX_RETRIES
    retry_delay = appsetting.SHARDING_LOG_RETRY_DELAY_SECONDS

    while attempt < max_retries:
        try:
            # Update the sync_field in the etl_task_table
            insert_log_data_query = upsert_sharding_log_sql(
                db_connection_config.target_db, sharding_table_log
            )

            # Prepare the data tuple for the query
            data_tuple = (
                sharding_table_log_opject.sharding_table_name,
                sharding_table_log_opject.shard_id,
                sharding_table_log_opject.source_db_name,
                sharding_table_log_opject.source_table_name,
                sharding_table_log_opject.id_field_name,
                sharding_table_log_opject.matchdate_field_name,
                sharding_table_log_opject.timestamp_field_name,
                sharding_table_log_opject.current_columns
            )

            # Execute the update query
            with target_connection.cursor() as cursor:
                cursor.execute(insert_log_data_query, data_tuple)
                target_connection.commit()

            return True

        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed: Error inserting log: {e}")
            
            if attempt >= max_retries:
                print("Max retries reached, failing operation.")
                return False
            else:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

    return False


def update_log_status(target_connection, db_connection_config, sharding_table_log_opject):
    attempt = 0
    max_retries = appsetting.SHARDING_LOG_MAX_RETRIES
    retry_delay = appsetting.SHARDING_LOG_RETRY_DELAY_SECONDS

    while attempt < max_retries:
        try:
            if sharding_table_log_opject.id_field_name != None:

                update_min_id_query = update_min_id_sql(
                    db_connection_config.target_db, sharding_table_log, sharding_table_log_opject
                )
                update_max_id_query = update_max_id_sql(
                    db_connection_config.target_db, sharding_table_log, sharding_table_log_opject
                ) 
                            
                # Execute the update query
                with target_connection.cursor() as cursor:
                    cursor.execute(update_min_id_query)
                    cursor.execute(update_max_id_query)
                    target_connection.commit()
                
            else:
                print('skip update id status')

            if sharding_table_log_opject.matchdate_field_name != None:

                update_min_matchdate_query = update_min_matchdate_sql(
                    db_connection_config.target_db, sharding_table_log, sharding_table_log_opject
                )
                update_max_matchdate_query = update_max_matchdate_sql(
                    db_connection_config.target_db, sharding_table_log, sharding_table_log_opject
                )    
                
                # Execute the update query
                with target_connection.cursor() as cursor:
                    cursor.execute(update_min_matchdate_query)
                    cursor.execute(update_max_matchdate_query)
                    target_connection.commit()
                
            else:
                print('skip update matchdate status')

            MIN_VALID_DATETIME = datetime(appsetting.MIN_VALID_DATETIME_YEAR, 1, 1)
            INVALID_DATETIME_STR = appsetting.INVALID_DATETIME_STR

            # filter timestamp as null
            if sharding_table_log_opject.timestamp_field_name is not None:
                min_timestamp = sharding_table_log_opject.min_timestamp
                max_timestamp = sharding_table_log_opject.max_timestamp

                # if value is null, set as None
                if min_timestamp == INVALID_DATETIME_STR or min_timestamp < MIN_VALID_DATETIME:
                    min_timestamp = None
                if max_timestamp == INVALID_DATETIME_STR or max_timestamp < MIN_VALID_DATETIME:
                    max_timestamp = None

                # update when timestamp value is not null
                if min_timestamp is not None or max_timestamp is not None:
                    update_min_timestamp_query = update_min_timestamp_sql(
                        db_connection_config.target_db, sharding_table_log
                    )
                    update_max_timestamp_query = update_max_timestamp_sql(
                        db_connection_config.target_db, sharding_table_log
                    )

                    # execute SQL command
                    with target_connection.cursor() as cursor:
                        if min_timestamp is not None:
                            cursor.execute(update_min_timestamp_query, (min_timestamp, sharding_table_log_opject.sharding_table_name, min_timestamp))
                        if max_timestamp is not None:
                            cursor.execute(update_max_timestamp_query, (max_timestamp, sharding_table_log_opject.sharding_table_name, max_timestamp))
                        target_connection.commit()
                else:
                    print("No valid timestamps, skipping update.")

            else:
                print("skip update timestamp status")

            return True

        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed: Error update log: {e}")
            
            if attempt >= max_retries:
                print("Max retries reached, failing operation.")
                return False
            else:
                print(f"Retrying in {retry_delay} seconds...")
                time.sleep(retry_delay)

    return False


def update_sync_field_in_etl_task_table(db_connection_config, etl_task_table):

    # Create source connection
    source_connection = db_connection_config.start_source_engine()
    target_connection = db_connection_config.start_target_engine()

    try:
        # Query to get all column names of the source table
        query_columns = select_source_column_names_sql(
            db_connection_config.source_db, db_connection_config.source_table
        )

        # Execute the query and fetch column names
        with source_connection.cursor() as cursor:
            cursor.execute(query_columns)
            columns = cursor.fetchall()

        # Create a comma-separated list of column names
        column_names = ', '.join([col[0] for col in columns])
        print(f"Detected columns: {column_names}")

        # Query to get the current sync_field value
        query_current_sync_field = select_sync_field_sql(
            target_db_config.db,
            etl_task_table,
            db_connection_config.source_db,
            db_connection_config.source_table,
        )

        with target_connection.cursor() as cursor:
            cursor.execute(query_current_sync_field)
            current_sync_field = cursor.fetchone()

        # Check if sync_field needs to be updated
        if current_sync_field and current_sync_field[0] == column_names:
            print("No changes detected in sync_field.")
            return False

        # Update the sync_field in the etl_task_table
        update_sync_field_query = update_sync_field_sql(
            target_db_config.db,
            etl_task_table,
            column_names,
            db_connection_config.source_db,
            db_connection_config.source_table,
        )

        # Execute the update query
        with target_connection.cursor() as cursor:
            cursor.execute(update_sync_field_query)
            target_connection.commit()

        print(f"Updated sync_field for table {db_connection_config.source_table} in {etl_task_table} successfully.")
        return True

    except Exception as e:
        print(f"Error updating sync_field: {e}")
    finally:
        # Close both connections
        db_connection_config.dispose_source_engine()
        db_connection_config.dispose_target_engine()

    return False


def check_table_column_comments(db_connection_config):

    # Create source and target connections
    source_connection = db_connection_config.start_source_engine()
    target_connection = db_connection_config.start_target_engine()

    try:
        # Query to get column definitions and comments from the source table, including length, precision, and scale
        query_columns = select_source_columns_with_comments_sql(
            db_connection_config.source_db, db_connection_config.source_table
        )

        query_table_comment = select_table_comment_sql(
            db_connection_config.source_db, db_connection_config.source_table
        )
        
        # Execute the query and fetch column definitions and comments from source table
        with source_connection.cursor() as cursor:
            cursor.execute(query_columns)
            columns = cursor.fetchall()
            cursor.execute(query_table_comment)
            table_comment = cursor.fetchone()[0]  # Get the table comment

        # Prepare list for column comments
        column_comments = {}

        # Store column comments and data types with precision/scale if applicable
        for col in columns:
            column_name = f"`{col[0]}`"  # Only wrap column name in a single pair of backticks
            column_type = col[1]  # column_type already contains the correct type (e.g., VARCHAR(50))
            max_length = col[2]
            precision = col[3]
            scale = col[4]
            column_comment = col[5]

            # If the column type already has parentheses (like VARCHAR(50), DECIMAL(10,2)), keep it as is
            if '(' in column_type and ')' in column_type:
                # If it already has parentheses, don't modify it
                pass
            elif 'decimal' in column_type.lower():
                # For DECIMAL type, ensure precision and scale are included
                if precision is not None and scale is not None:
                    column_type = f"{column_type}({precision},{scale})"
                else:
                    column_type = f"{column_type}"  # For DECIMAL without precision/scale
            elif 'varchar' in column_type.lower() or 'char' in column_type.lower():
                # For VARCHAR/CHAR types, include max length if present, but avoid double parentheses
                if max_length is not None:
                    column_type = f"{column_type}({max_length})"
            # For other types, ensure the correct handling
            else:
                column_type = f"{column_type}"

            # Store the column comment
            column_comments[column_name] = {'type': column_type, 'comment': column_comment}

        # Get the list of target tables
        query_target_tables = select_sharded_target_tables_sql(
            db_connection_config.target_db, db_connection_config.source_table
        )

        with target_connection.cursor() as cursor:
            cursor.execute(query_target_tables)
            target_tables = cursor.fetchall()

        # Iterate over each target table and update the comments
        for target_table in target_tables:
            target_table_name = target_table[0]
            print(f"Updating comments for table `{target_table_name}`...")

            # Update the table comment
            if table_comment:
                alter_table_comment_query = alter_table_comment_sql(
                    db_connection_config.target_db, target_table_name, table_comment
                )
                try:
                    with target_connection.cursor() as cursor:
                        cursor.execute(alter_table_comment_query)
                        target_connection.commit()
                    print(f"Updated table comment for `{target_table_name}`.")
                except pymysql.MySQLError as e:
                    print(f"Failed to update table comment for `{target_table_name}`: {e}")

            # Update column comments
            for column_name, column_info in column_comments.items():
                column_type = column_info['type']
                column_comment = column_info['comment']
                if column_comment:
                    # Ensure column name is wrapped in a single pair of backticks only
                    alter_column_comment_query = alter_column_comment_sql(
                        db_connection_config.target_db,
                        target_table_name,
                        column_name,
                        column_type,
                        column_comment,
                    )
                    try:
                        with target_connection.cursor() as cursor:
                            cursor.execute(alter_column_comment_query)
                            target_connection.commit()
                        print(f"Updated comment for column `{column_name}` in table `{target_table_name}`.")
                    except pymysql.MySQLError as e:
                        print(f"Failed to update comment for column `{column_name}` in table `{target_table_name}`: {e}")

    except Exception as e:
        print(f"Error updating comments: {e}")
    finally:
        # Close both connections
        db_connection_config.dispose_source_engine()
        db_connection_config.dispose_target_engine()



def alter_target_tables_if_schema_changed(db_connection_config):

    # Create source and target connections
    source_connection = db_connection_config.start_source_engine()
    target_connection = db_connection_config.start_target_engine()

    try:
        # Query to get all column definitions from the source table
        query_columns = select_source_column_types_sql(
            db_connection_config.source_db, db_connection_config.source_table
        )

        # Execute the query and fetch column definitions from source table
        with source_connection.cursor() as cursor:
            cursor.execute(query_columns)
            columns = cursor.fetchall()

        # Create a dictionary to store column definitions
        column_definitions = {}
        for col in columns:
            column_name = col[0]
            column_type = col[1]
            column_definition = f"{column_type}"  # You can extend this to include length, precision etc. like before
            
            # Add additional constraints if necessary
            if col[5] == "YES":
                column_definition += " NULL"
            else:
                column_definition += " NOT NULL"

            if col[6] is not None:
                column_definition += f" DEFAULT {col[6]}"

            column_definitions[column_name] = column_definition.strip()

        # Get the list of target tables that need to be altered
        query_target_tables = select_sharded_target_tables_sql(
            db_connection_config.target_db, db_connection_config.source_table
        )

        with target_connection.cursor() as cursor:
            cursor.execute(query_target_tables)
            target_tables = cursor.fetchall()

        # Iterate over each target table and alter it to match the source table schema
        for target_table in target_tables:
            target_table_name = target_table[0]
            print(f"Altering table {target_table_name} to match source schema...")

            # Ensure the table schema matches before continuing
            for column_name, column_definition in column_definitions.items():
                # Check if the column already exists in the target table
                check_column_query = column_exists_sql(
                    db_connection_config.target_db, target_table_name, column_name
                )
                with target_connection.cursor() as cursor:
                    cursor.execute(check_column_query)
                    column_exists = cursor.fetchone()[0]

                # If the column does not exist, add it
                if column_exists == 0:
                    alter_table_query = add_column_sql(
                        db_connection_config.target_db,
                        target_table_name,
                        column_name,
                        column_definition,
                    )
                    try:
                        with target_connection.cursor() as cursor:
                            cursor.execute(alter_table_query)
                            target_connection.commit()
                        print(f"Added column `{column_name}` to table `{target_table_name}` successfully.")
                    except pymysql.MySQLError as e:
                        print(f"Failed to add column `{column_name}` to table `{target_table_name}`: {e}")
                else:
                    print(f"Column `{column_name}` already exists in table `{target_table_name}`, skipping.")

    except Exception as e:
        print(f"Error altering target tables: {e}")
    finally:
        # Close both connections
        db_connection_config.dispose_source_engine()
        db_connection_config.dispose_target_engine()


def execute_sql_with_params( db_connection , query, db_connection_config, params=None, if_return=False):
    
    # choose db to connect
    if db_connection == 'source':
        connection = db_connection_config.start_source_engine()
    elif db_connection == 'target':
        connection = db_connection_config.start_target_engine()
        
    try:
        # if need to return data
        if if_return:
            df = pd.read_sql(query, connection, params=params)
            df.reset_index(drop=True, inplace=True)
            return df
        
        # just send sql to execute
        else:
            with connection.cursor() as cursor:
                cursor.execute(query, params)  
                connection.commit()  

    except Exception as e:
        print(f"Error executing query: {e}")

        if if_return:
            return pd.DataFrame()
                
    finally:
        # close db connection
        if db_connection == 'source':
            db_connection_config.dispose_source_engine()
        elif db_connection == 'target':
            db_connection_config.dispose_target_engine()


def create_target_sharding_table_if_not_exist(source_data, db_connection_config):
    # create both source & target connection
    source_connection = db_connection_config.start_source_engine()
    target_connection = db_connection_config.start_target_engine()
    sync_field = db_connection_config.sync_field

    # Prepare columns_list for SQL select, filter out problematic fields like 'ignore'
    formatted_sync_fields = ', '.join([f"`{field.strip()}`" for field in sync_field.split(',') if field.strip().lower() != 'ignore'])

    # Correctly format sync fields for use in SQL query (wrap in single quotes)
    fields_for_query = ', '.join([f"'{field.strip()}'" for field in sync_field.split(',') if field.strip().lower() != 'ignore'])

    # Get columns datatype
    query_datatype = select_column_datatypes_sql(
        db_connection_config.source_db, db_connection_config.source_table, fields_for_query
    )
    
    try:
        # Open source connection cursor
        with source_connection.cursor() as cursor:

            cursor.execute(query_datatype)
            columns = cursor.fetchall()
        
        # Prepare list for column definitions
        column_definitions = []

        # Generate target table schema through source table schema
        for col in columns:

            column_name = f"`{col[0]}`"
            # Determine column definition type and its details
            if col[1].lower() in ['varchar', 'char']:
                column_def = f"{column_name} {col[1]}" + (f"({col[2]})" if col[2] is not None else '')
            elif col[1].lower() == 'decimal':
                column_def = f"{column_name} {col[1]}" + (f"({col[3]},{col[4]})" if col[3] is not None and col[4] is not None else '')
            else:
                column_def = f"{column_name} {col[1]}"

            # Add PRIMARY KEY if it's the primary key field, ensuring that col[0] is not None and pk_field is not pandas.NA
            if col[0] is not None and db_connection_config.pk_field is not pd.NA and col[0] == db_connection_config.pk_field:
                column_def += " PRIMARY KEY"

            # Append to column definitions list
            column_definitions.append(column_def)

        # Use , to split list 
        column_definitions_sql = ', '.join(column_definitions)
        # Add timestamp column to track ETL update time
        timestamp_column = "`etl_update_time` TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
        column_definitions_sql += f", {timestamp_column}"
        
        # Partition_num for balancing table load
        partition_num_list = source_data['partition_num'].dropna().unique().tolist()

        # Generate SQL to create target tables if they do not exist
        for partition_num in partition_num_list:
            try:

                if db_connection_config.etl_type in [1, 2, 3]:
                    create_table_sql = create_sharded_table_sql(
                        db_connection_config.target_db,
                        f"{db_connection_config.target_table}_{partition_num}",
                        column_definitions_sql,
                    )
                elif db_connection_config.etl_type == 4:
                    create_table_sql = create_sharded_table_sql(
                        db_connection_config.target_db,
                        db_connection_config.target_table,
                        column_definitions_sql,
                    )

                # Execute table creation SQL
                with target_connection.cursor() as cursor:
                    cursor.execute(create_table_sql)
                    target_connection.commit()

            except pymysql.MySQLError as e:
                print(f"Error creating table partition {partition_num}: {e}")
                # Depending on your use case, you might want to continue, re-raise the error, or handle it differently.

    except pymysql.MySQLError as e:
        print(f"Error during create_target_sharding_table_if_not_exist: {e}")

    finally:
        # Close both source & target connections
        db_connection_config.dispose_source_engine()
        db_connection_config.dispose_target_engine()



def add_indexes_to_target_sharding_table(source_data, db_connection_config):
    # create both source & target connection
    source_connection = db_connection_config.start_source_engine()
    target_connection = db_connection_config.start_target_engine()

    try:
        # get indexes from source table
        query_indexes = select_source_indexes_sql(
            db_connection_config.source_db, db_connection_config.source_table
        )

        # execute the query to fetch indexes from source table
        with source_connection.cursor() as cursor:
            cursor.execute(query_indexes)
            indexes = cursor.fetchall()

        # prepare a dictionary to hold index columns
        index_columns = {}
        for index in indexes:
            index_name = index[0]
            column_name = index[1]
            non_unique = index[2]

            if index_name not in index_columns:
                index_columns[index_name] = {
                    "columns": [],
                    "non_unique": non_unique
                }
            index_columns[index_name]["columns"].append(column_name)

        # prepare index creation statements for each partitioned target table
        partition_num_list = source_data['partition_num'].unique().tolist()

        for partition_num in partition_num_list:
            target_table_name = f"`{db_connection_config.target_db}`.`{db_connection_config.target_table}_{partition_num}`"

            for index_name, index_info in index_columns.items():
                columns = index_info["columns"]
                non_unique = index_info["non_unique"]

                # Escape index name and column names to avoid SQL syntax issues
                escaped_index_name = f"`{index_name}`"
                escaped_columns = ', '.join([f"`{col}`" for col in columns])

                # Check if index already exists on target table
                check_index_query = index_exists_sql(
                    db_connection_config.target_db,
                    f"{db_connection_config.target_table}_{partition_num}",
                    index_name,
                )

                with target_connection.cursor() as cursor:
                    cursor.execute(check_index_query)
                    index_exists = cursor.fetchone()[0]

                # If index does not exist, create it
                if index_exists == 0:
                    # Determine if the index is unique or non-unique
                    unique = '' if non_unique else 'UNIQUE'

                    # Create index SQL statement
                    index_ddl = create_index_sql(
                        unique, escaped_index_name, target_table_name, escaped_columns
                    )
                    
                    print(f"Executing SQL: {index_ddl}")

                    try:
                        with target_connection.cursor() as cursor:
                            cursor.execute(index_ddl)
                            target_connection.commit()
                    except pymysql.MySQLError as e:
                        raise RuntimeError(f"Failed to create index '{index_name}' on table '{target_table_name}': {e}")

    finally:
        # close both source & target connection
        db_connection_config.dispose_source_engine()
        db_connection_config.dispose_target_engine()




def is_json_serializable(value):
    try:
        json.dumps(value)
        return True
    except (TypeError, ValueError):
        return False


def compress_ids_and_load_into_mysql(ids_list, db_connection_config):
    if not ids_list:
        print("No ids to compress. Skipping compression.")
        return

    # Prepare the ids as a string
    if isinstance(ids_list[0], str):
        formatted_ids = ','.join([f"'{str(id)}'" for id in ids_list])
    elif isinstance(ids_list[0], int):
        formatted_ids = ','.join([str(id) for id in ids_list])
    else:
        raise ValueError(f"Unsupported primary key field type: {ids_list[0]}")

    # Compress the formatted ids using zlib
    compressed_ids = zlib.compress(formatted_ids.encode('utf-8'))

    # SQL insert statement with placeholders
    query = insert_compressed_ids_sql(
        initial_etl_task_connection_config.target_db, etl_sync_ids
    )
    
    # Execute the query using parameters to safely insert the compressed data
    execute_sql_with_params('target', query, db_connection_config, params=(
        db_connection_config.source_db,
        db_connection_config.source_table,
        compressed_ids  # This will now be inserted correctly as binary
    ))


def distribute_data_and_load(source_data, db_connection_config): 

    # prepare a list to record the id of successful processing
    ids_list = []

    # Initialize `filtered_fields`
    sync_fields = db_connection_config.sync_field.split(',')
    sync_fields = [field.strip() for field in sync_fields]

    # Filter out 'ignore' fields
    filtered_fields = [field for field in sync_fields if field != '`ignore`']

    if not source_data.empty and db_connection_config.etl_type in [1, 2, 3]:
        
        source_data.reset_index(drop=True, inplace=True)
        source_data = source_data.replace({np.nan: None})  
        
        # Sharding source_data by partition_num
        grouped = source_data.groupby('partition_num')
        dataframes = {}

        # Group data by partition number
        for partition_num, staging_table in grouped:
            staging_table.reset_index(drop=True, inplace=True)
            dataframes[f'{db_connection_config.target_table}_{partition_num}'] = staging_table

        target_connection = db_connection_config.start_target_engine()

        # Column names and placeholders for SQL insertion
        column_names = ', '.join([f'`{field}`' for field in filtered_fields])
        placeholder_names = ', '.join(['%s' for _ in filtered_fields])

        for distribute_target_table, staging_data in dataframes.items():

            current_sharding_table_log_opject = gen_empty_sharding_table_log_opject()
            current_sharding_table_log_opject.sharding_table_name = distribute_target_table
            current_sharding_table_log_opject.shard_id = int(distribute_target_table.split('_')[-1])
            current_sharding_table_log_opject.source_db_name = db_connection_config.source_db
            current_sharding_table_log_opject.source_table_name = db_connection_config.source_table
            current_sharding_table_log_opject.id_field_name = None if pd.isna(db_connection_config.id_field_name) else db_connection_config.id_field_name
            if current_sharding_table_log_opject.id_field_name is not None:
                current_sharding_table_log_opject.min_id = min(staging_data[f'{current_sharding_table_log_opject.id_field_name}'].dropna())
                current_sharding_table_log_opject.max_id = max(staging_data[f'{current_sharding_table_log_opject.id_field_name}'].dropna())
            current_sharding_table_log_opject.matchdate_field_name = None if pd.isna(db_connection_config.event_date_field_name) else db_connection_config.event_date_field_name
            if current_sharding_table_log_opject.matchdate_field_name is not None:
                current_sharding_table_log_opject.min_matchdate = min(staging_data[f'{current_sharding_table_log_opject.matchdate_field_name}'].dropna())
                current_sharding_table_log_opject.max_matchdate = max(staging_data[f'{current_sharding_table_log_opject.matchdate_field_name}'].dropna())
            current_sharding_table_log_opject.timestamp_field_name = None if pd.isna(db_connection_config.timestamp_field_name) else db_connection_config.timestamp_field_name
            if current_sharding_table_log_opject.timestamp_field_name is not None:
                current_sharding_table_log_opject.min_timestamp = min(staging_data[f'{current_sharding_table_log_opject.timestamp_field_name}'].dropna())
                current_sharding_table_log_opject.max_timestamp = max(staging_data[f'{current_sharding_table_log_opject.timestamp_field_name}'].dropna())
            current_sharding_table_log_opject.latest_update_time = None
            current_sharding_table_log_opject.current_columns = column_names

            insert_log_data(target_connection, db_connection_config, current_sharding_table_log_opject)

            # Update clause for on-duplicate key update
            update_clause = ', '.join([f"`{col}`=VALUES(`{col}`)" for col in filtered_fields if col != db_connection_config.pk_field])

            insert_sql = upsert_sharded_row_sql(
                db_connection_config.target_db,
                distribute_target_table,
                column_names,
                placeholder_names,
                update_clause,
            )
            # reset index 
            staging_data.reset_index(drop=True, inplace=True)

            # open target cursor
            with target_connection.cursor() as cursor:
                # try every row
                for _, row in staging_data.iterrows():
                    # check field value instance
                    for field in filtered_fields:
                        if field in row.index:
                            if pd.isnull(row[field]) or row[field] == '':
                                # set default value based on data type
                                if isinstance(row[field], int):  # int
                                    row[field] = 0
                                elif isinstance(row[field], float):  # float
                                    row[field] = 0.0
                                elif isinstance(row[field], str):  # str
                                    row[field] = ''
                                else:
                                    row[field] = None
                            elif is_json_serializable(row[field]) and isinstance(row[field], (dict, list)):
                                row[field] = json.dumps(row[field])
                            else:
                                row[field] = row[field]
                    
                    # prepare row ready to insert
                    filtered_row = tuple(None if pd.isnull(row[field]) else row[field] for field in filtered_fields if field in row.index)

                    try:
                        cursor.execute(insert_sql, filtered_row)
                        if db_connection_config.pk_field in row and db_connection_config.if_need_house_keeping == 1:
                            ids_list.append(row[db_connection_config.pk_field])
                    except pymysql.MySQLError as e:
                        if "Field" in str(e) and "doesn't have a default value" in str(e):
                            # if a column is missing a default value, try to alter the table schema
                            print(f"Column missing. Attempting to alter target table structure: {str(e)}")
                            alter_target_tables_if_schema_changed(db_connection_config)
                            # retry the insert after altering the table
                            cursor.execute(insert_sql, filtered_row)
                        else:
                            # Log error and continue with other rows to avoid complete failure
                            print(f"Error inserting row: {filtered_row}. Error: {e}")
                    
                update_log_status(target_connection, db_connection_config, current_sharding_table_log_opject)
                    
                # Commit after each batch insertion
                target_connection.commit()
                current_sharding_table_log_opject.reset()

        # close target connection
        db_connection_config.dispose_target_engine()

        if db_connection_config.if_need_house_keeping == 1: 
            # insert and compress ids_list into id_table
            compress_ids_and_load_into_mysql(ids_list, db_connection_config)
    
    elif db_connection_config.etl_type == 4 and not source_data.empty:
        # Create target connection
        target_connection = db_connection_config.start_target_engine()
        
        try:
            # prepare sql to delete old data
            truncate_sql = truncate_table_sql(
                db_connection_config.target_db, db_connection_config.target_table
            )

            column_names = ', '.join(filtered_fields)
            placeholder_names = ', '.join(['%s'] * len(filtered_fields))

            insert_sql = insert_row_sql(
                db_connection_config.target_db,
                db_connection_config.target_table,
                column_names,
                placeholder_names,
            )
            
            # open target cursor
            with target_connection.cursor() as cursor:
                # execute delete sql
                cursor.execute(truncate_sql)
                
                # insert data
                for _, row in source_data.iterrows():
                    filtered_row = tuple(None if pd.isnull(row[field]) else row[field] for field in filtered_fields if field in row.index)
                    cursor.execute(insert_sql, filtered_row)
                
                # commit target change
                target_connection.commit()
        
        finally:
            # target connection close
            db_connection_config.dispose_target_engine()

    
def update_etl_task_table(set_value, db_connection_config):
    
    # record status by instance
    if isinstance(set_value, np.integer):
        set_value = int(set_value)
    
    target_connection = db_connection_config.start_target_engine() 

    if isinstance(set_value, datetime):
        set_column = 'last_timestamp_sync_record'
        set_value = f"'{set_value.strftime('%Y-%m-%d %H:%M:%S')}'"  
    elif isinstance(set_value, int):
        set_column = 'last_id_sync_record'
    else:
        set_column = 'last_id_sync_record'
        set_value = zlib.crc32(set_value.encode('utf-8'))  

    update_sql = update_watermark_sql(
        target_db_config.db,
        etl_task_table,
        set_column,
        set_value,
        db_connection_config.target_db,
        db_connection_config.target_table,
    )
    
    try:
        with target_connection.cursor() as cursor: 
            cursor.execute(update_sql)
            # commit target change
            target_connection.commit()  
    finally:
        # close target connection
        db_connection_config.dispose_target_engine()  


def update_etl_task_value_by_etl_type( source_data,current_etl_task_connection_config): # record status by etl_type
    
    if current_etl_task_connection_config.etl_type == 1:
        current_etl_task_connection_config.last_id_sync_record = source_data[current_etl_task_connection_config.id_field_name].max()
        update_etl_task_table(current_etl_task_connection_config.last_id_sync_record, current_etl_task_connection_config)
            
    elif current_etl_task_connection_config.etl_type == 2 or current_etl_task_connection_config.etl_type == 3:
        current_etl_task_connection_config.last_timestamp_sync_record = pd.to_datetime(source_data[current_etl_task_connection_config.timestamp_field_name].max())
        current_etl_task_connection_config.last_id_sync_record = source_data[current_etl_task_connection_config.id_field_name].max()
        update_etl_task_table(current_etl_task_connection_config.last_timestamp_sync_record, current_etl_task_connection_config)
        update_etl_task_table(current_etl_task_connection_config.last_id_sync_record, current_etl_task_connection_config)
            
    elif current_etl_task_connection_config.etl_type == 4:
        current_etl_task_connection_config.last_timestamp_sync_record = datetime.now()
        update_etl_task_table(current_etl_task_connection_config.last_timestamp_sync_record, current_etl_task_connection_config)
    
# get task from etl_task_table
def etl_tasks(airflow_task_id, etl_task_table, db_connection_config):
    query = select_active_sync_tasks_sql(target_db_config.db, etl_task_table, airflow_task_id)
    
    target_engine = db_connection_config.start_target_engine()
    
    with target_engine.cursor() as cursor:  
        cursor.execute(query)
        tasks = cursor.fetchall()

        columns = [col[0] for col in cursor.description]  
        tasks_df = pd.DataFrame(tasks, columns=columns) 
        tasks_df.reset_index(drop=True, inplace=True)

    db_connection_config.dispose_target_engine()  
    
    return tasks_df

# main job
def process_etl_table(current_etl_task_connection_config):
    
    current_timestamp_tag = timestamp_tag(None)

    check_sharding_table_log_exists(current_etl_task_connection_config)

    loop_count = 0

    if_columns_change = update_sync_field_in_etl_task_table(current_etl_task_connection_config, etl_task_table)

    if if_columns_change:

        target_connection = current_etl_task_connection_config.start_target_engine()
        try:
            query_updated_sync_field = select_sync_field_sql(
                target_db_config.db,
                etl_task_table,
                current_etl_task_connection_config.source_db,
                current_etl_task_connection_config.source_table,
            )
            with target_connection.cursor() as cursor:
                cursor.execute(query_updated_sync_field)
                updated_sync_field = cursor.fetchone()[0]
                current_etl_task_connection_config.sync_field = updated_sync_field
                print(f"Updated sync_field in current_etl_task_connection_config: {updated_sync_field}")

        except Exception as e:
            print(f"Error retrieving updated sync_field: {e}")
        finally:
            current_etl_task_connection_config.dispose_target_engine()

        alter_target_tables_if_schema_changed(current_etl_task_connection_config)
    else:
        print(f"No column change in {current_etl_task_connection_config.source_db}.{current_etl_task_connection_config.source_table}, skip alter process")

    check_table_column_comments(current_etl_task_connection_config)

    while True:

        source_sql = generate_query_sql(current_etl_task_connection_config,current_timestamp_tag)
        source_data = execute_sql_with_params('source',source_sql, current_etl_task_connection_config, params=None, if_return=True)
        
        if source_data.empty:
            print(f"No more data in {current_etl_task_connection_config.source_db}.{current_etl_task_connection_config.source_table} to process.")
            break
        
        create_target_sharding_table_if_not_exist(source_data, current_etl_task_connection_config)
        
        if current_etl_task_connection_config.etl_type != 4:
            add_indexes_to_target_sharding_table(source_data, current_etl_task_connection_config)

        retry_count = 0
        while retry_count < appsetting.DISTRIBUTE_MAX_RETRIES:
            try:
                distribute_data_and_load(source_data, current_etl_task_connection_config)
                break  # Break the retry loop if successful
            except Exception as e:
                print(f"Error occurred in distribute_data_and_load: {e}. Retrying in {appsetting.DISTRIBUTE_RETRY_SLEEP_SECONDS} seconds...")
                retry_count += 1
                if retry_count < appsetting.DISTRIBUTE_MAX_RETRIES:
                    time.sleep(appsetting.DISTRIBUTE_RETRY_SLEEP_SECONDS)
                else:
                    print("Maximum retry attempts reached. Moving on to the next task.")

        update_etl_task_value_by_etl_type(source_data, current_etl_task_connection_config)
        
        loop_count += 1
        if loop_count >= current_etl_task_connection_config.max_loop_count:
            
            break
        
    etl_pause_msg = f"Pause {current_etl_task_connection_config.source_db}.{current_etl_task_connection_config.source_table} sync job after {loop_count} loops. ({loop_count}/{current_etl_task_connection_config.max_loop_count})"
    send_msg_to_multiple_slack_channel(etl_pause_msg)


def break_msg(current_etl_task_connection_config):
    
    break_msg = f"{current_etl_task_connection_config.source_db}.{current_etl_task_connection_config.source_table} not found, check next task"
    send_msg_to_multiple_slack_channel(break_msg)


airflow_task_id = Variable.get("airflow_task_id")
etl_task_table = appsetting.etl_task_table_name(airflow_task_id)
sharding_table_log = appsetting.SHARDING_TABLE_LOG
etl_sync_ids = appsetting.etl_sync_ids_table_name(airflow_task_id)
crypto_key = Variable.get("crypto_key")
is_config_updated = Variable.get("is_config_updated")
is_poc = Variable.get("is_poc")

target_db_config = db_config(appsetting.mysql_account_key(is_poc))
proxy = target_db_config.proxy
init_slack(proxy)
initial_etl_task_connection_config = etl_task_connection_config.from_target_db_config(
    target_db_config, crypto_key
)

check_and_update_etl_task_record(etl_task_table, initial_etl_task_connection_config, is_config_updated)

tasks_df = etl_tasks(airflow_task_id, etl_task_table, initial_etl_task_connection_config)
tasks_df.replace(["nan", "NULL"], pd.NA, inplace=True)

with DAG(
    appsetting.MYSQL_TO_MYSQL_DAG_ID,
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
    },
    description="Daily tasks for backing up data between mysqls",
    schedule_interval=appsetting.MYSQL_TO_MYSQL_SCHEDULE,
    catchup=False,
    tags=appsetting.MYSQL_TO_MYSQL_TAGS,
) as dag:

    starting_stage_op = PythonOperator(
            task_id=f"starting_stage",
            python_callable=starting_stage,
            trigger_rule=TriggerRule.ALL_DONE
        )
    
    previous_task = None
    
    for index, row in tasks_df.iterrows():
        
        current_etl_task_connection_config = create_etl_task_connection_config(row, crypto_key)
        task_id_prefix = f"{current_etl_task_connection_config.etl_type}_{current_etl_task_connection_config.source_db}_{current_etl_task_connection_config.source_table}"

        branch_op = BranchPythonOperator(
            task_id=f"{task_id_prefix}_branch_check_table_exists",
            python_callable=check_table_exists,
            op_kwargs={'db_connection_config': current_etl_task_connection_config},
            trigger_rule=TriggerRule.ALL_DONE
        )
        
        break_stage = PythonOperator(
            task_id=f"{task_id_prefix}_break_stage",
            python_callable=break_msg,
            op_kwargs={'current_etl_task_connection_config': current_etl_task_connection_config},
            trigger_rule=TriggerRule.ALL_DONE
        )

        process_table_op = PythonOperator(
            task_id=f"{task_id_prefix}_process",
            python_callable=process_etl_table,
            op_kwargs={'current_etl_task_connection_config': current_etl_task_connection_config},
            trigger_rule=TriggerRule.ALL_DONE
        )

        
        if previous_task:
            previous_task >> branch_op
        
        else:
            starting_stage_op >> branch_op
        
        branch_op >> [process_table_op, break_stage]  

        previous_task = [process_table_op, break_stage]

            
