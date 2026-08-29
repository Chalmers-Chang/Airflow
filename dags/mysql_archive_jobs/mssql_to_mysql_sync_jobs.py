import json
import time

import pandas as pd
import pymysql
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.trigger_rule import TriggerRule

from common.variables import ensure_all_project_variables

import general.toolbox as tb
from classes.sync_config import SyncConfig
from config import appsetting
from config.db_config import db_config
from slack.notifier import init_slack, send_msg_to_multiple_slack_channel
from sql.mssql.control import (
    create_etl_task_record_sp_sql,
    etl_task_record_sp_exists_sql,
    select_active_mssql_tasks_sql,
    update_watermark_sql,
    upsert_etl_task_record_sp_sql,
)
from sql.mssql.load import upsert_row_sql
from sql.mssql.stored_procedure import generate_query_sql_statement_by_config

"""
======================================================================
2025-02-25@Chalmers: Created DAG to sync data from MSSQL to MySQL
2025-03-04@Chalmers: Added ETL_type = 2, query by timestamp
======================================================================
"""

ensure_all_project_variables()

is_poc = Variable.get("is_poc")
is_config_table_updated = Variable.get("is_config_table_updated")
airflow_task_id = Variable.get("airflow_task_id")
crypto_key = Variable.get("crypto_key")


def starting_stage() -> None:
    
    if is_poc == "1":
        etl_start_msg = f'============= (Ignore this message) Test running etl_tasks based on {etl_task_table} ============='
    else:
        etl_start_msg = f'============= start running etl_tasks based on {etl_task_table} ============='
    
    send_msg_to_multiple_slack_channel(etl_start_msg)


def check_and_update_etl_task_record(target_db, etl_task_table, mysql_config) -> None:

    # create target connection
    target_connection = pymysql.connect(
            host=mysql_config.host, # mysql_config.host
            # port=3307, # local
            user=mysql_config.username,
            password=tb.password_decode(crypto_key,mysql_config.password),
            db=target_db
    )
    print("Successfully connected to the MySQL database.")

    # prepare task table
    check_task_table_sql = etl_task_record_sp_exists_sql(etl_task_table)

    # open target cursor
    with target_connection.cursor() as cursor: 
        cursor.execute(check_task_table_sql)
        result_task = cursor.fetchone()[0]  
        print(f'result_task:{result_task}')

        # if task table not exist, create one by airflow_task_id
        if result_task == 0:
            print("Checked: No rows returned from query")
            create_task_table_sql = create_etl_task_record_sp_sql(target_db, etl_task_table)

            with target_connection.cursor() as cursor:  
                cursor.execute(create_task_table_sql)
                target_connection.commit()
                print(f"New: Create new table {etl_task_table} in {target_db}")

    csv_file_path = appsetting.config_file(appsetting.ETL_TASK_RECORD_SP_CSV)
    etl_task_record_df = pd.read_csv(csv_file_path, index_col=False)

    for _, row in etl_task_record_df.iterrows():
        insert_sql = upsert_etl_task_record_sp_sql(target_db, etl_task_table, row)
        
        # close target cursor
        with target_connection.cursor() as cursor:  
            cursor.execute(insert_sql)
            target_connection.commit()

    # close target connection 
    if target_connection: 
            target_connection.close()

    Variable.set('is_config_table_updated', '0')


# get task from etl_task_table
def etl_tasks(airflow_task_id, target_db, etl_task_table, mysql_config) -> pd.DataFrame:
    query = select_active_mssql_tasks_sql(target_db_config.db, etl_task_table, airflow_task_id)
    
    target_engine = pymysql.connect(
            host=mysql_config.host, # mysql_config.host
            # port=3307, # local 
            user=mysql_config.username,
            password=tb.password_decode(crypto_key,mysql_config.password),
            db=target_db
    )
    
    with target_engine.cursor() as cursor:  
        cursor.execute(query)
        tasks = cursor.fetchall()

        columns = [col[0] for col in cursor.description]  
        tasks_df = pd.DataFrame(tasks, columns=columns) 
        tasks_df.reset_index(drop=True, inplace=True)

    if target_engine:
            target_engine.close()
    
    return tasks_df


# execute stored procedure to get data from mssql
def exec_sp_to_get_data_from_mssql(conn, config, SQL_statement, params) -> pd.DataFrame:

    try:
        cursor = conn.cursor()
        cursor.execute(SQL_statement, params)
        rows = cursor.fetchall()

        if not rows:
            print("MSSQL Query executed but returned no data.")
            return pd.DataFrame()

        columns = [column[0] for column in cursor.description]

        data_dicts = []
        for row in rows:
            # convert row to dictionary
            row_dict = {col: val for col, val in zip(columns, row)}
            data_dicts.append(row_dict)

        data = pd.DataFrame(data_dicts)
        data['SourceHost'] = config.source_host
        data['SourceDB'] = config.source_db

    except Exception as e:
        print(f"MSSQL failed to query: {e}")
        return pd.DataFrame()

        return data


def load_column_rules(json_file) -> dict:
    with open(json_file, "r", encoding="utf-8") as file:
        return json.load(file)


# generate column rules
def generate_column_rules(config, json_rule) -> str:
    if json_rule["target_db"] == config.target_db and json_rule["target_table"] == config.target_table:
        return json_rule["covert_data_type"]
    return None


# convert rule for dataframe
def convert_rule_for_dataframe(dataframe, config, json_rule) -> pd.DataFrame:
    for rule in json_rule:
        column_name = rule["column_name"]
        target_type = generate_column_rules(config, rule)

        if target_type and column_name in dataframe.columns:

            if target_type == "bit":
                dataframe[column_name] = dataframe[column_name].apply(lambda x: 1 if x == True else 0)

            elif target_type == "bigint":
                dataframe[column_name] = dataframe[column_name].apply(
                    lambda x: int.from_bytes(x, byteorder="big") if isinstance(x, bytes) else x
                )

    return dataframe


# insert dataframe into mysql
def insert_dataframe_into_mysql(target_conn, dataframe, config) -> None:

    # convert NaN to None
    dataframe = dataframe.where(pd.notnull(dataframe), None)
    # convert Null to None
    dataframe = dataframe.applymap(lambda x: None if x == "Null" or pd.isnull(x) else x)
    # convert digit from str to int
    dataframe = dataframe.applymap(lambda x: int(x) if isinstance(x, str) and x.isdigit() else x)

    conn = target_conn
    if conn is None:
        print("MySQL failed to connect.")
        return

    try:
        with conn.cursor() as cursor:
            for _, row in dataframe.iterrows():
                insert_sql = upsert_row_sql(config.target_db, config.target_table, row.index)

                try:
                    cleaned_row = [None if pd.isna(value) else value for col, value in row.items() if col != 'inserttime']
                    cursor.execute(insert_sql, tuple(cleaned_row))
                except pymysql.MySQLError as e:
                    print(f"Error inserting or updating row in MySQL: {e}")
                    continue

            conn.commit()
            print("Data successfully written to MySQL.")
    except Exception as e:
        print(f"MySQL failed to query: {e}")


# update id or timestamp field value in record table
def update_id_field_value_in_record_table(target_conn, target_field, update_value, id_field_name, config) -> None:

    conn = target_conn
    if conn is None:
        print("MySQL failed to connect.")
        return

    try:
        with conn.cursor() as cursor:
            
            insert_sql = update_watermark_sql(
                target_db_config.db,
                etl_task_table,
                target_field,
                update_value,
                config.source_table_id,
            )

            cursor.execute(insert_sql)
            conn.commit()
            print(f"{target_field} - {id_field_name} : {update_value} updated successfully.")

    except Exception as e:
        print(f"MySQL failed to update: {repr(e)}")


# update etl task config through record table
def set_etl_task_config(row) -> SyncConfig:
    return SyncConfig(
        source_table_id=row['source_table_id'],
        airflow_task_id=row['airflow_task_id'],
        is_active=row['is_active'],
        source_username=row['source_username'],
        source_password=row['source_password'],
        source_host=row['source_host'],            
        source_db=row['source_db'],
        source_table=row['source_table'],
        source_sp=row['source_sp'],
        etl_type=row['etl_type'],
        arg_counts=row['arg_counts'],
        target_username=row['target_username'],
        target_password=row['target_password'],
        target_host=row['target_host'],
        target_db=row['target_db'],
        target_table=row['target_table'],
        pk_field=row['pk_field'],
        sync_field=row['sync_field'],
        max_loop_count=row['max_loop_count'],
        batch_size=row['batch_size'],
        id_field_name_1=None if pd.isna(row['id_field_name_1']) else row['id_field_name_1'],
        last_id_sync_record_1=row['last_id_sync_record_1'],
        id_field_name_2=None if pd.isna(row['id_field_name_2']) else row['id_field_name_2'],
        last_id_sync_record_2=row['last_id_sync_record_2'],
        timestamp_field_name=None if pd.isna(row['timestamp_field_name']) else row['timestamp_field_name'],
        last_timestamp_sync_record=None if pd.isna(row['last_timestamp_sync_record']) else int(row['last_timestamp_sync_record']),
        crypto_key=crypto_key,
    )


# close source and target engine, and send message to slack channel
def end_process(task_config,loop_count) -> None:
    task_config.mssql_dispose_source_engine()
    task_config.mysql_dispose_target_engine()     
    etl_pause_msg = f"Pause {task_config.source_host}.{task_config.source_db}.{task_config.source_table} sync job after {loop_count} loops. ({loop_count}/{task_config.max_loop_count})"
    send_msg_to_multiple_slack_channel(etl_pause_msg)


# main etl process
def etl_process(task_config, json_rule) -> None:

    print(f"========= Starting {task_config.source_sp} ETL process =========")

    # start source and target engine
    source_conn = task_config.mssql_start_source_engine()
    target_conn = task_config.mysql_start_target_engine()
    
    # counter and flag
    loop_count = 0
    if_no_more_result = 0

    # loop to get data from mssql and insert into mysql
    while loop_count < task_config.max_loop_count and if_no_more_result == 0 :
        
        loop_count += 1
        print(f"({loop_count}/{task_config.max_loop_count}) ETL process")
        time.sleep(appsetting.ETL_LOOP_SLEEP_SECONDS)
        dataframe = pd.DataFrame()

        # Source: MSSQL
        try:
            # generate SQL statement
            SQL_statement_query, params = generate_query_sql_statement_by_config(task_config)
            # execute stored procedure to get data from mssql
            dataframe = exec_sp_to_get_data_from_mssql(source_conn, task_config, SQL_statement_query, params)
            # convert specific data type according to the rules in the json file
            convert_rule_for_dataframe(dataframe, task_config, json_rule)

            if dataframe.empty:
                print("No data retrieved from MSSQL.")
                if_no_more_result = 1
                end_process(task_config,loop_count)
                return  

        except Exception as e:
            print(f"MSSQL ERROR: {e}")
            end_process(task_config,loop_count)       
            return  

        try:
            if not dataframe.empty:
                # Target: MySQL
                insert_dataframe_into_mysql(target_conn, dataframe, task_config)

                # update last id_field_1 value in record table
                if task_config.id_field_name_1 and task_config.id_field_name_1 != 'nan':
                    task_config.last_id_sync_record_1 = max(dataframe[task_config.id_field_name_1])     
                    if task_config.etl_type != 4:
                        update_id_field_value_in_record_table(target_conn, 'last_id_sync_record_1', task_config.last_id_sync_record_1, task_config.id_field_name_1, task_config)  

                # update last id_field_2 value in record table
                if task_config.id_field_name_2 and task_config.id_field_name_2 != 'nan':
                    task_config.last_id_sync_record_2 = max(dataframe[task_config.id_field_name_2])
                    if task_config.etl_type != 4:
                        update_id_field_value_in_record_table(target_conn, 'last_id_sync_record_2', task_config.last_id_sync_record_2, task_config.id_field_name_2, task_config)

                # update last timestamp_field value in record table
                if task_config.timestamp_field_name and task_config.timestamp_field_name != 'nan':
                    task_config.last_timestamp_sync_record = int(max(dataframe[task_config.timestamp_field_name]))
                    update_id_field_value_in_record_table(target_conn, 'last_timestamp_sync_record', task_config.last_timestamp_sync_record, task_config.timestamp_field_name, task_config)

        except Exception as e:
            print(f"MySQL ERROR: {repr(e)}")

    # send message to slack channel
    end_process(task_config,loop_count)


target_db_config = db_config(appsetting.mysql_account_key(is_poc))
proxy = target_db_config.proxy
init_slack(proxy)
record_target_db = target_db_config.db
etl_task_table = appsetting.MSSQL_ETL_TASK_TABLE
json_file_path = appsetting.config_file(appsetting.CONVERT_RULE_JSON)
json_rule = load_column_rules(json_file_path)

# check and update etl task record table if is_config_table_updated is 1
if is_config_table_updated == '1':
    check_and_update_etl_task_record(record_target_db,etl_task_table, target_db_config)

# get tasks from etl_task_table
tasks_df = etl_tasks(airflow_task_id, record_target_db, etl_task_table, target_db_config)
tasks_df.replace(['nan', 'NULL'], pd.NA, inplace=True)

# define DAG
with DAG(
    appsetting.MSSQL_TO_MYSQL_DAG_ID,
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
    },
    description="Daily tasks for synchronizing tables from MSSQL to MySQL",
    schedule_interval=appsetting.MSSQL_TO_MYSQL_SCHEDULE,
    catchup=False,
    tags=appsetting.MSSQL_TO_MYSQL_TAGS,
) as dag:

    # send message to slack channel before starting etl process
    starting_stage_op = PythonOperator(
            task_id=f"starting_stage",
            python_callable=starting_stage,
            trigger_rule=TriggerRule.ALL_DONE
        )

    previous_task = None

    # loop through tasks_df to create etl process
    for index, row in tasks_df.iterrows():
        
        # set etl task config
        task_config = set_etl_task_config(row)
        # set task_id_prefix
        task_id_prefix = f"{task_config.source_table_id}_{task_config.source_db}_{task_config.source_sp}"

        # create main etl process task
        process_table_op = PythonOperator(
            task_id=f"{task_id_prefix}_process",
            python_callable=etl_process,
            op_kwargs={
                'task_config': task_config,
                'json_rule': json_rule,
            },
            trigger_rule=TriggerRule.ALL_DONE
        )

        if previous_task:
            previous_task >> process_table_op
        else:    
            starting_stage_op

        previous_task = process_table_op