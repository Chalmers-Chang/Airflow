from google.cloud import bigquery
from google.oauth2 import service_account
import json
import os
import pyodbc
import pandas as pd
from google.auth.transport.requests import Request
import base64
from cryptography.fernet import Fernet
 

def safe_sql_value(value) -> str:
    if value is None or pd.isnull(value) or value == "":
        return "NULL"
    if isinstance(value, bytes):
        return str(int.from_bytes(value, byteorder="big"))
    if isinstance(value, str):
        return f"'{value}'"
    return str(value)


def password_decode(crypto_key, pass_word_encode):

    if pass_word_encode != None:
        key = base64.urlsafe_b64encode(crypto_key.encode())
        cipher_suite = Fernet(key)
        decrypted_text = cipher_suite.decrypt(pass_word_encode.encode())
        return decrypted_text.decode()
    
    else :
        return None
    
  
def load_json_config(file_path):
    # open json
    with open(file_path, 'r') as file:
        config = json.load(file)
    return config


def execute_sql_statement_on_bigquery(credentials_info,sqlcommand):

    # Construct a Credentials object
    credentials = service_account.Credentials.from_service_account_info(credentials_info)
    # BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials_info["project_id"])
    # EXEC
    query_job = client.query(sqlcommand)
    # Result output
    results = query_job.result()
    return results


def query_data_from_bigquery(proxy, credentials_info, sql_statement):
    # proxy out going
    if proxy is not None:
        os.environ['http_proxy'] = proxy
        os.environ['https_proxy'] = proxy
    else:
        print("Warning: 'proxy' is None, skipping setting proxy environment variables.")

    # Construct a Credentials object with scope for BigQuery
    credentials = service_account.Credentials.from_service_account_info(
        credentials_info, 
        scopes=["https://www.googleapis.com/auth/cloud-platform"]
    )
    
    # Check if the credentials need to be refreshed
    if not credentials.valid or credentials.expired:
        try:
            print("Credentials are not valid or expired, attempting to refresh...")
            credentials.refresh(Request())
            print("Credentials successfully refreshed.")
        except Exception as e:
            print(f"Error refreshing credentials: {e}")
            raise

    # Initialize BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials_info["project_id"])

    # SQL query
    query = sql_statement

    # Execute query
    try:
        query_job = client.query(query)
        results = query_job.result()  # Blocks until the query completes
        print(f"Successfully executed SQL: {sql_statement}")
    except Exception as e:
        print(f"Error executing SQL query: {e}")
        raise

    return results



def upload_dataframe_to_bigquery(proxy, credentials_info,data,dataset,tablename,write_disposition):
    
    if proxy != None:
        # proxy out going
        os.environ['http_proxy'] = proxy
        os.environ['https_proxy'] = proxy
        
    # Construct a Credentials object
    credentials = service_account.Credentials.from_service_account_info(credentials_info)

    # BigQuery client
    client = bigquery.Client(credentials=credentials, project=credentials_info["project_id"])

    # target table
    dataset_id = dataset #'Test'
    table_id = tablename #'test'
    table_ref = client.dataset(dataset_id).table(table_id)

    # get table ref
    table = client.get_table(table_ref)

    # upload data 
    job_config = bigquery.LoadJobConfig(write_disposition=write_disposition)
    job_config.schema = table.schema

    load_job = client.load_table_from_dataframe(data, table_ref, job_config=job_config)  

    # finish job
    load_job.result()

    print('Upload Successfully')


def connect_to_sql(server, database, username, password):
    conn_str = f'DRIVER={{ODBC Driver 17 for SQL Server}};SERVER={server};DATABASE={database};UID={username};PWD={password}'
    return pyodbc.connect(conn_str)


def mssql_sql_query(server,database,username,password,sql_commend):
  
    # SQL Server Connect
    conn = connect_to_sql(server, database, username, password)
    cursor = conn.cursor()
    
    # exec stored procedure
    sql_query = f"""{sql_commend};
    """
    cursor.execute(sql_query)
    
    rows = cursor.fetchall()
    
    # get column names
    columns = [column[0] for column in cursor.description]
    
    # create empty dictionary
    data_dicts = []
    
    # transform rows datatype to dictionary
    for row in rows:
        row_dict = {col: val for col, val in zip(columns, row)}
        data_dicts.append(row_dict)
    
    # insert Data into DataFrame
    data = pd.DataFrame(data_dicts)
    
    # Connnection Close
    cursor.close()
    conn.close()

    return data