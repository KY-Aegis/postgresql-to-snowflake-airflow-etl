#import libraries
from airflow import DAG
from airflow.providers.postgres.operators.postgres import PostgresHook
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine
import json
import os

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 6, 1),
    'retries': 1,
}

dag = DAG('read_json', default_args=default_args, schedule_interval='@daily')

def generate_json_file ():
    # Json Data to Track the Max Primary Key after every execution
    data = [
        {
        "table": "Customers_Table",
        "primary_key": "customer_id",
        "index": 0
        },
        {
        "table": "Products_Table",
        "primary_key": "product_id",
        "index": 0
        },
        {
        "table": "Orders_Table",
        "primary_key": "order_id",
        "index": 0
        },
        {
        "table": "Sales_Data_Table",
        "primary_key": "sale_id",
        "index": 0
        }
    ]

    filename = 'primary_key_validator.json' #name for the json file
    
    if not os.path.exists(filename): #check if file exists
        with open(filename, 'w') as json_file:# Create a JSON file in the current directory
            json.dump(data, json_file, indent=4) 

extract_task = PythonOperator(
    task_id='validate_json_file',
    python_callable=generate_json_file,
    dag=dag
)

extract_task
# # Database connection details for PostgreSQL
# db_user = 'postgres'
# db_password = 'abc123'
# db_host = 'localhost'
# db_port = '5432'
# db_name = 'Test1'

# #create the connection
# engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

# customers = pd.read_sql('SELECT * FROM Customers_Table', engine)

# print(customers)



