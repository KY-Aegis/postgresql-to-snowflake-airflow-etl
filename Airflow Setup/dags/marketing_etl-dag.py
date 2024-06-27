# Import libraries
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from sqlalchemy import create_engine
from airflow import DAG
import pandas as pd

# Define airflow dag
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Configure airflow to run every hour
dag = DAG('marketing_data_etl', default_args=default_args, schedule_interval='@daily')

# Function to process data retrieved from JSON
def extract_latest_data(ti):
    # Database connection details for PostgreSQL
    db_user = 'postgres'
    db_password = 'password'
    db_host = 'postgres'
    db_port = '5432'
    db_name = 'moneylion_assignment'

    # Create the sql connection
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')
    
    # Select today's data
    query = f'''SELECT * FROM marketing_team_data where "sale_date" = date(now()- interval '1 day');'''

        
    # Push data to XCom
    ti.xcom_push(key='latest_marketing_data', value=pd.read_sql(query, engine)) 
    
    # Logging
    print('Data Queried Successfully')
    
# Function to load data to Snowflake
def load_data(ti):
    # Retrieve data from task instance with specific key
    table_data = ti.xcom_pull(key='latest_marketing_data', task_ids='extract_latest_data')

    # Initialize the snowflake connection   
    snowflake_hook = SnowflakeHook(snowflake_conn_id='Moneylion_Snowflake_Warehouse')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Use a specific schema
    cursor.execute("USE SCHEMA PUBLIC") 

    # Create the tables if it does not exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS Marketing_Team_Data (
                       "product_name" STRING,
                        "product_description" STRING,
                        "sale_date" DATE,
                        "sale_hour" INT,
                        "state" STRING, 
                        "city" STRING,
                        "total_amount" FLOAT,
                        "total_quantity" INT
                    );''')  

    success, nchunks, nrows, _ = write_pandas(conn, table_data, 'Marketing_Team_Data'.upper())

    print('Sync Successful!')
    
    # End the connection
    cursor.close()
    conn.close()


extract_task = PythonOperator(
    task_id='extract_latest_data',
    python_callable=extract_latest_data,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

extract_task>>load_task
