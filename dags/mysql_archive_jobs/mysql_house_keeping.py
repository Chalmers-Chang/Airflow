import time
from datetime import datetime, timedelta, timezone

import pandas as pd
from airflow import DAG
from airflow.models import Variable
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.utils.dates import days_ago
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule

from classes.etl_task_config import create_etl_task_connection_config, etl_task_connection_config
from config import appsetting
from config.db_config import db_config
from slack.notifier import init_slack, send_msg_to_multiple_slack_channel
from sql.mysql.control import select_house_keeping_tasks_sql
from sql.mysql.house_keeping import (
    delete_expired_sync_ids_sql,
    delete_source_by_ids_sql,
    mark_sync_ids_deleted_sql,
    optimize_table_sql,
    select_pending_sync_ids_sql,
    select_recent_source_ids_sql,
)
import zlib

# Chalmers 2024-12-04 20:59 commit


def etl_tasks(airflow_task_id, etl_task_table, db_connection_config):
    query = select_house_keeping_tasks_sql(
        initial_etl_task_connection_config.target_db, etl_task_table, airflow_task_id
    )
    
    target_engine = db_connection_config.start_target_engine()
    
    with target_engine.cursor() as cursor:  
        cursor.execute(query)
        tasks = cursor.fetchall()

        columns = [col[0] for col in cursor.description]  
        tasks_df = pd.DataFrame(tasks, columns=columns) 
        tasks_df.reset_index(drop=True, inplace=True)

    db_connection_config.dispose_target_engine()  
    
    return tasks_df

def starting_stage():
    #
    if is_poc:
        etl_start_msg = f'============= (Ignore) start testing house_keeping_tasks on mysql ============='
        send_msg_to_multiple_slack_channel(etl_start_msg)   
    else:
        etl_start_msg = f'============= start running house_keeping_tasks on mysql ============='
        send_msg_to_multiple_slack_channel(etl_start_msg)   
            

def execute_sql(db_connection, query, db_connection_config, if_return=False):
    if db_connection == 'source':
        connection = db_connection_config.start_source_engine()
    elif db_connection == 'source_house_keeping':
        connection = db_connection_config.start_source_house_keeping_engine()        
    elif db_connection == 'target':
        connection = db_connection_config.start_target_engine()

    if if_return:
        df = pd.read_sql(query, connection)
        df.reset_index(drop=True, inplace=True)
        if db_connection == 'source':
            db_connection_config.dispose_source_engine()
        elif db_connection == 'source_house_keeping':
            db_connection_config.dispose_source_house_keeping_engine()            
        elif db_connection == 'target':    
            db_connection_config.dispose_target_engine()
        return df
    else:
        with connection.cursor() as cursor:
            cursor.execute(query)
            connection.commit()  
        if db_connection == 'source':
            db_connection_config.dispose_source_engine()
        elif db_connection == 'source_house_keeping':
            db_connection_config.dispose_source_house_keeping_engine()            
        elif db_connection == 'target':    
            db_connection_config.dispose_target_engine()
        return True
    

def get_ids_compress_list_from_etl_sync_ids (db_connection_config):
    
    delete_sql = delete_expired_sync_ids_sql(
        initial_etl_task_connection_config.target_db,
        etl_sync_ids,
        appsetting.HOUSE_KEEPING_EXPIRED_DAYS,
    )

    query = select_pending_sync_ids_sql(
        initial_etl_task_connection_config.target_db,
        etl_sync_ids,
        date_sub_start_day,
        date_sub_end_day,
        db_connection_config.source_db,
        db_connection_config.source_table,
    )
    

    execute_sql( 'target' , delete_sql, db_connection_config, if_return=False)
    df = execute_sql( 'target' , query, db_connection_config, if_return=True)
    if not df.empty:
        df.columns = ['house_keeping_id' ,'ids']
    
    return df


def check_if_synced_id_list_is_expired_over_14_days(db_connection_config, synced_id_list):

    query = select_recent_source_ids_sql(
        db_connection_config.source_db,
        db_connection_config.source_table,
        db_connection_config.timestamp_field_name,
        db_connection_config.id_field_name,
        synced_id_list,
        appsetting.HOUSE_KEEPING_EXPIRED_DAYS,
    )

    df = execute_sql('source_house_keeping', query, db_connection_config, if_return=True)

    return df.empty


def delete_source_data_by_synced_ids(db_connection_config, synced_id_list, house_keeping_id):
    
    query = delete_source_by_ids_sql(
        db_connection_config.source_db,
        db_connection_config.source_table,
        db_connection_config.id_field_name,
        synced_id_list,
    )
    
    attempt = 0
    max_retries = appsetting.HOUSE_KEEPING_MAX_RETRIES
    success = False
    
    while attempt < max_retries and not success:
        try:
            execute_sql('source_house_keeping', query, db_connection_config, if_return=False)
            
            success = True
            return_msg = f'house_keeping_id_{house_keeping_id}_done'
            return return_msg
        
        except Exception as e:
            attempt += 1
            print(f"Attempt {attempt} failed: {e}. Retrying in 10 seconds...")
            time.sleep(appsetting.HOUSE_KEEPING_RETRY_SLEEP_SECONDS)
    
    if not success:
        return f'house_keeping_id_{house_keeping_id}_failed_after_{max_retries}_attempts'
    
    
def update_etl_sync_ids_status(db_connection_config, house_keeping_id):
    
    query = mark_sync_ids_deleted_sql(
        initial_etl_task_connection_config.target_db, etl_sync_ids, house_keeping_id
    )
    
    attempt = 0
    max_retries = appsetting.HOUSE_KEEPING_MAX_RETRIES
    success = False
    
    while attempt < max_retries and not success:
        try:

            execute_sql('target', query, db_connection_config, if_return=False)
            
            success = True
            return_msg = f'house_keeping_id_{house_keeping_id}_status_updated'
            return return_msg
        
        except Exception as e:
            
            attempt += 1
            print(f"Attempt {attempt} failed: {e}. Retrying in 10 seconds...")
            time.sleep(appsetting.HOUSE_KEEPING_RETRY_SLEEP_SECONDS)
    
    if not success:
        print(f'house_keeping_id_{house_keeping_id}_status_update_failed_after_{max_retries}_attempts')
        return f'house_keeping_id_{house_keeping_id}_status_update_failed_after_{max_retries}_attempts'

def decompressed_data(ids):
    try:
        decompressed_data = zlib.decompress(ids)
        ids_decode = decompressed_data.decode('utf-8')  
        return ids_decode
    except (zlib.error, UnicodeDecodeError) as e:
        print(f"Decompression or decoding error: {e}")
        return None

def house_keeping_process(db_connection_config):

    try:
        print(f'source_password:{db_connection_config.source_password}')
        ids_to_be_delete_list = get_ids_compress_list_from_etl_sync_ids(db_connection_config)
        
        if not ids_to_be_delete_list.empty:
            df_not_empty_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：Start deleting data through records in {etl_sync_ids}'
            send_msg_to_multiple_slack_channel(df_not_empty_msg) 
            
            for index, row in ids_to_be_delete_list.iterrows():
                
                house_keeping_id = row['house_keeping_id']
                ids = row['ids']
                
                # 
                try:
                    synced_id_list = decompressed_data(ids)
                    if not synced_id_list:
                        error_msg = f"Decompression failed for house_keeping_id {house_keeping_id}, skipping."
                        send_msg_to_multiple_slack_channel(error_msg)
                        continue 

                except Exception as decompress_error:
                    error_msg = f"Error decompressing data for house_keeping_id {house_keeping_id}: {str(decompress_error)}"
                    send_msg_to_multiple_slack_channel(error_msg)
                    raise decompress_error  # 
                
                is_expired  = check_if_synced_id_list_is_expired_over_14_days(db_connection_config, synced_id_list)
                print(synced_id_list[:20])
                if is_expired  == True:
                    # 
                    try:
                        # fake_msg = f"this process will excute house_keeping_id:{house_keeping_id}, delete {db_connection_config.source_db}.{db_connection_config.source_table} from id:{synced_id_list[:20]}..."
                        # send_msg_to_multiple_slack_channel(fake_msg)
                        delete_source_data_by_synced_ids(db_connection_config, synced_id_list, house_keeping_id)

                    except Exception as delete_error:
                        error_msg = f"Error deleting source data for house_keeping_id {house_keeping_id}: {str(delete_error)}"
                        send_msg_to_multiple_slack_channel(error_msg)
                        raise delete_error  # 
                    
                     
                    try:
                        success = update_etl_sync_ids_status(db_connection_config, house_keeping_id)
                        if not success:
                            error_msg = f"Failed to update status for house_keeping_id {house_keeping_id}."
                            send_msg_to_multiple_slack_channel(error_msg)

                    except Exception as update_error:
                        error_msg = f"Error updating etl_sync_ids status for house_keeping_id {house_keeping_id}: {str(update_error)}"
                        send_msg_to_multiple_slack_channel(error_msg)
                        raise update_error  # 
                else:
                    do_not_delete_msg = f"house_keeping_id:{house_keeping_id} is not expired, skip house-keeping process {synced_id_list[:20]}..."
                    print(do_not_delete_msg)

                time.sleep(appsetting.HOUSE_KEEPING_SLEEP_BETWEEN_ROWS_SECONDS)

            df_empty_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：House-Keeping completed, no expired data remains to be deleted'
            send_msg_to_multiple_slack_channel(df_empty_msg)

        else:
            skip_task_msg = f'skip {db_connection_config.source_db}.{db_connection_config.source_table} hosue keeping process'
            send_msg_to_multiple_slack_channel(skip_task_msg)       

        return 'No need to delete data anymore'

    except Exception as e:
        error_msg = f"Error in house_keeping_process: {str(e)}"
        send_msg_to_multiple_slack_channel(error_msg)
        raise e  # 


def get_time_setting():
    
    # set time zone as UTC+8
    utc_8 = timezone(timedelta(hours=8))
    current_time = datetime.now(utc_8)

    # get date
    today = current_time.date()

    # switch time_str into datetime format
    start_time = datetime.combine(today, datetime.strptime(optimize_start_time_str, "%H:%M").time()).replace(tzinfo=utc_8)
    end_time = datetime.combine(today, datetime.strptime(optimize_end_time_str, "%H:%M").time()).replace(tzinfo=utc_8)    
    
    return utc_8, current_time, start_time, end_time

def calculate_wait_time_seconds(current_time, start_time):

    wait_time = (start_time - current_time).total_seconds() + 1
    
    return max(0, int(wait_time))

def optimize_starting_stage():
    #
    
    if is_optimization_active == 0:
        optimize_pause_msg = f'============= is_optimization_active == 0, pause optimization tasks today ============='
        send_msg_to_multiple_slack_channel(optimize_pause_msg)       
        
    else:
        optimize_start_msg = f'============= Waiting for {optimize_start_time_str} to run optimization tasks on mysql ============='
        send_msg_to_multiple_slack_channel(optimize_start_msg)   
        
        utc_8, current_time, start_time, end_time = get_time_setting()
        wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)
        
        while current_time < start_time:
            wait_msg = f"Current time is {current_time.time()}. Waiting {wait_time_seconds} seconds until {start_time.time()} to start optimization."
            send_msg_to_multiple_slack_channel(wait_msg)        
            time.sleep(wait_time_seconds)
            
            current_time = datetime.now(utc_8)
            wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)


def optimize_source_table(db_connection_config):

    utc_8, current_time, start_time, end_time = get_time_setting()
    wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)
    
    # wait for optimization
    while current_time < start_time:
        time.sleep(wait_time_seconds)
        
        current_time = datetime.now(utc_8)
        wait_time_seconds = calculate_wait_time_seconds(current_time, start_time)

    # optimize duration 
    if start_time <= current_time <= end_time:
        optimize_start_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：Start running the optimization process'
        send_msg_to_multiple_slack_channel(optimize_start_msg)

        try:
            query = optimize_table_sql(
                db_connection_config.source_db, db_connection_config.source_table
            )
            result = execute_sql('source_house_keeping', query, db_connection_config, if_return=True)
            print("Database Result:", result)
            
            optimize_end_msg = f'[ {db_connection_config.source_db}.{db_connection_config.source_table} ]：Optimization completed'
            send_msg_to_multiple_slack_channel(optimize_end_msg)


        except Exception as e:
            error_msg = f'Error occurred while optimizing {db_connection_config.source_db}.{db_connection_config.source_table}: {e}'
            send_msg_to_multiple_slack_channel(error_msg)

    else:
        # over 12:00 skip
        skip_msg = f"Current time is {current_time.time()}. Outside the allowed range of {start_time.time()} to {end_time.time()} UTC+8. Skipping optimization process."
        send_msg_to_multiple_slack_channel(skip_msg)

    return True

def decide_optimization():
    if is_optimization_active == 1:
        return "optimize_starting_stage" 
    else:
        return "skip_optimization_stage"


airflow_task_id = Variable.get("airflow_task_id")
is_optimization_active = int(Variable.get("is_optimization_active"))
optimize_start_time_str = Variable.get("optimize_start_time_str")
optimize_end_time_str = Variable.get("optimize_end_time_str")
date_sub_start_day = Variable.get("mysql_house_keeping_date_sub_start_day")
date_sub_end_day = Variable.get("mysql_house_keeping_date_sub_end_day")
crypto_key = Variable.get("crypto_key")

etl_task_table = appsetting.etl_task_table_name(airflow_task_id)
etl_sync_ids = appsetting.etl_sync_ids_table_name(airflow_task_id)
is_poc = Variable.get("is_poc")

target_db_config = db_config(appsetting.mysql_account_key(is_poc))
if is_poc == "1":
    proxy = None
else:
    proxy = target_db_config.proxy
init_slack(proxy)

initial_etl_task_connection_config = etl_task_connection_config.from_target_db_config(
    target_db_config, crypto_key
)

tasks_df = etl_tasks(airflow_task_id, etl_task_table, initial_etl_task_connection_config)

with DAG(
    appsetting.MYSQL_HOUSE_KEEPING_DAG_ID,
    default_args={
        "owner": "airflow",
        "start_date": days_ago(1),
    },
    description="Perform housekeeping tasks based on the id records in the etl_sync_ids table ",
    schedule_interval=appsetting.MYSQL_HOUSE_KEEPING_SCHEDULE,
    catchup=False,
    tags=appsetting.MYSQL_HOUSE_KEEPING_TAGS,
) as dag:

    # housekeeping start
    housekeeping_starting_stage_op = PythonOperator(
        task_id="housekeeping_starting_stage",
        python_callable=starting_stage,
        trigger_rule=TriggerRule.ALL_DONE 
    )

    # BranchPythonOperator
    decide_optimization_op = BranchPythonOperator(
        task_id='decide_optimization',
        python_callable=decide_optimization,
    )

    # optimization_start
    optimize_starting_stage_op = PythonOperator(
        task_id="optimize_starting_stage",
        python_callable=optimize_starting_stage,
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
    )

    # housekeeping_tasks_group 
    with TaskGroup("housekeeping_tasks_group") as housekeeping_tasks_group:
        previous_task = None
        for index, row in tasks_df.iterrows():
            db_connection_config = create_etl_task_connection_config(row, crypto_key)
            task_id_prefix = f"{db_connection_config.etl_type}_{db_connection_config.source_db}_{db_connection_config.source_table}"

            current_task = PythonOperator(
                task_id=f"{task_id_prefix}_house_keeping_process",
                python_callable=house_keeping_process,
                op_kwargs={'db_connection_config': db_connection_config},
                trigger_rule=TriggerRule.ALL_DONE
            )
        
            if previous_task:
                previous_task >> current_task

            previous_task = current_task

    # housekeeping finished
    all_housekeeping_done = EmptyOperator(
        task_id="all_housekeeping_done",
        trigger_rule=TriggerRule.ALL_DONE
    )

    # optimize_tasks_group 
    with TaskGroup("optimize_tasks_group") as optimize_tasks_group:
        for index, row in tasks_df.iterrows():
            db_connection_config = create_etl_task_connection_config(row, crypto_key)
            task_id_prefix = f"{db_connection_config.etl_type}_{db_connection_config.source_db}_{db_connection_config.source_table}"

            PythonOperator(
                task_id=f"{task_id_prefix}_optimize_process",
                python_callable=optimize_source_table,
                op_kwargs={'db_connection_config': db_connection_config},
                trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS
            )

    # skip optimization
    skip_optimization_stage = EmptyOperator(
        task_id="skip_optimization_stage"
    )

    # skip optimization
    end_optimization_stage = EmptyOperator(
        task_id="end_optimization_stage"
    )



    # dependency
    housekeeping_starting_stage_op >> housekeeping_tasks_group >> all_housekeeping_done >> decide_optimization_op
    decide_optimization_op >> [optimize_starting_stage_op, skip_optimization_stage] 
    optimize_starting_stage_op >> optimize_tasks_group
    skip_optimization_stage >> end_optimization_stage