# Import libraries
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from snowflake.connector.pandas_tools import write_pandas
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from cryptography.fernet import Fernet
from sqlalchemy import create_engine
from airflow import DAG
import pandas as pd
import json
import os

# Define airflow dag
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1,
}

# Configure airflow to run every hour
dag = DAG('postgres_to_snowflake_etl', default_args=default_args, schedule_interval='@hourly')

# Function to validate if the JSON file exists
def validate_json_file():
    # Json to track max primary key of each table after every successful execution
    data = [
        { "table": "Customers_Table","primary_key": "customer_id","index": 0 },
        { "table": "Products_Table","primary_key": "product_id","index": 0 },
        { "table": "Orders_Table","primary_key": "order_id","index": 0 },
        { "table": "Sales_Data_Table","primary_key": "sale_id","index": 0 }
    ]

    # File directory
    filename = './primary_key_validator.json'
    
    # Check if file exists
    if not os.path.exists(filename):  
        # Create a JSON file in the specified directory
        with open(filename, 'w') as json_file:  
            # Overwrite the file
            json.dump(data, json_file, indent=4)

         # Logging 
        print("JSON file created successfully.")
    # If file already exists
    else: 
        # Logging
        print("JSON file already exists.")

# Function to retrieve data from JSON
def read_json_file(ti):
        # File directory
        filename = './primary_key_validator.json'
        
        # Read file 
        with open(filename, 'r') as json_file:
            # Retrieve data 
            table_data = json.load(json_file)
        # Logging 
        print("Data read from JSON file:") 

        # Push data to XCom
        ti.xcom_push(key='json_data', value=table_data) 

# Function to process data retrieved from JSON
def extract_latest_data(ti):
    # Database connection details for PostgreSQL
    db_user = 'postgres'
    db_password = 'password'
    db_host = 'postgres'
    db_port = '5432'
    db_name = 'moneylion_assignment'

    # Retrieve data from task instance with specific key
    table_data = ti.xcom_pull(key='json_data', task_ids='read_json_file') 

    # Create the sql connection
    engine = create_engine(f'postgresql://{db_user}:{db_password}@{db_host}:{db_port}/{db_name}')

    # Loop through the JSON array
    for table in table_data:
        #construct the query based on the JSON data
        query = f'SELECT * FROM {table["table"]} where {table["primary_key"]}>{table["index"]};'
        # Push data to XCom
        ti.xcom_push(key=table["table"], value=pd.read_sql(query, engine)) 
    
    # Logging
    print('Data Queried Successfully')

# Function to transform data retrieved from PostgreSQL
def transform_data(ti):
    # Retrieve data from task instance with specific key
    table_data = ti.xcom_pull(key='json_data', task_ids='read_json_file')
        
    # Loop through JSON array
    for table in table_data:
        # Retrieve DataFrame from XCom
        df = ti.xcom_pull(key=table['table'], task_ids='extract_postgres_data')
        if not df.empty:# Run only if the data frame is not empty
            # Convert timestamp to string to avoid Invalid Date in snowflake
            df['created_on'] = df['created_on'].astype(str)
            df['updated_on'] = df['updated_on'].astype(str)
            
                # If table name is orders
            if table['table'] == 'Orders_Table':
                df[['quantity']] = df[['quantity']].fillna(value=0)  # Fill NaN values in 'quantity' with 0
                
            # If table name is customers
            elif table['table'] == 'Customers_Table':
                # List to store normalized address
                normalized_df = []
                    
                # Normalize address data
                for index, row in df.iterrows():
                    customer_id = row['customer_id']
                    address_lists = row['address']
                        
                    # Iterate over each address list
                    for address_list in address_lists:
                        # Convert each address_list into a DataFrame
                        address_df = pd.DataFrame([address_list])  # Wrap address_list in a list
                        address_df['customer_id'] = customer_id
                        normalized_df.append(address_df)

                # Flatten the dataframe into a single df
                flatten_df = pd.concat(normalized_df, ignore_index=True)
                    
                # Drop address from Customer Table
                df = df.drop('address', axis=1)
                    
                # Push data to XCom
                ti.xcom_push(key='Customer_Address_Table', value=flatten_df)

                # Generate a key for encryption to secure PII data
                key = Fernet.generate_key()
                cipher_suite = Fernet(key)

                # Encrypt the name and email of customers for PII
                df['name'] = df['name'].apply(lambda x: cipher_suite.encrypt(x.encode()).decode())
                df['email'] = df['email'].apply(lambda x: cipher_suite.encrypt(x.encode()).decode())
                ti.xcom_push(key='fernet_key', value=str(key.decode()))

        # Push data to XCom
        ti.xcom_push(key=table['table'], value=df )

# Function to insert updated encryption keys
def load_rotated_keys(ti):
    df = ti.xcom_pull(key='Customers_Table', task_ids='extract_postgres_data')
    if not df.empty:# Run only if the data frame is not empty
        fernet_key = ti.xcom_pull(key='fernet_key', task_ids='transform_data') 

        # Initialize the snowflake connection   
        snowflake_hook = SnowflakeHook(snowflake_conn_id='Moneylion_Snowflake_Warehouse')
        conn = snowflake_hook.get_conn()
        cursor = conn.cursor()

        # Use a specific schema
        cursor.execute("USE SCHEMA PUBLIC") 

        # Create the tables if it does not exist
        cursor.execute('''CREATE TABLE IF NOT EXISTS Encryption_Keys (
                            "key_id" INT AUTOINCREMENT PRIMARY KEY,
                            "encrypted_key" STRING,
                            "created_at" TIMESTAMP DEFAULT CURRENT_TIMESTAMP()
                        );
                        ''')  
        # Insert the encryption keys
        cursor.execute(f'''INSERT INTO Encryption_Keys ("encrypted_key") VALUES ('{str(fernet_key)}')''')

        # Get the latest key id
        cursor.execute('SELECT MAX("key_id") FROM encryption_keys;')
        
        # Fetch the result
        max_id = cursor.fetchone()[0]

        ti.xcom_push(key='encrypt_key_index', value=str(max_id)) 

        # Close the connection
        cursor.close()
        conn.close()

        print('Keys Rotated Succesfully!')
    
# Function to load data to Snowflake
def load_data(ti):
    # Retrieve data from task instance with specific key
    table_data = ti.xcom_pull(key='json_data', task_ids='read_json_file')

    # Initialize the snowflake connection   
    snowflake_hook = SnowflakeHook(snowflake_conn_id='Moneylion_Snowflake_Warehouse')
    conn = snowflake_hook.get_conn()
    cursor = conn.cursor()

    # Use a specific schema
    cursor.execute("USE SCHEMA PUBLIC") 

    # Create the tables if it does not exist
    cursor.execute('''CREATE TABLE IF NOT EXISTS Customers_Table (
                       "customer_id" INT PRIMARY KEY,
                        "name" STRING,
                        "email" STRING,
                        "created_on" TIMESTAMP,
                        "updated_on" TIMESTAMP, 
                        "encryption_key_index" INT
                    );''')  
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Products_Table (
                        "product_id" INTEGER PRIMARY KEY,
                        "name" STRING,
                        "description" STRING,
                        "price" FLOAT,
                        "created_on" TIMESTAMP,
                        "updated_on" TIMESTAMP  
                    );''')  
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Orders_Table (
                        "order_id" INTEGER PRIMARY KEY,
                        "customer_id" INTEGER,
                        "product_id" STRING,
                        "quantity" INTEGER,
                        "order_date" DATE,
                        "created_on" TIMESTAMP,
                        "updated_on" TIMESTAMP  
                    );''') 
    
    cursor.execute('''CREATE TABLE IF NOT EXISTS Sales_Data_Table (
                        "sale_id" INTEGER PRIMARY KEY,
                        "order_id" INT,
                        "total_amount" FLOAT,
                        "sale_date" DATE,
                        "created_on" TIMESTAMP,
                        "updated_on" TIMESTAMP  
                    );''')  

    cursor.execute('''CREATE TABLE IF NOT EXISTS Customer_Address (
                        "address_id" INTEGER AUTOINCREMENT PRIMARY KEY,
                        "customer_id" INTEGER ,
                        "Street" STRING,
                        "City" STRING,
                        "State" STRING,
                        "Zip" STRING
                    );''')  

    for table in table_data:
        # Retrieve DataFrame from XCom
        df = ti.xcom_pull(key=table['table'], task_ids='transform_data')
        if not df.empty:# Run only if the data frame is not empty
            print(f'Loading data into Snowflake for : {table["table"]}')

            if table["table"]=='Customers_Table':
                # Get encryption keys reference
                max_id = ti.xcom_pull(key='encrypt_key_index', task_ids='load_key') 
                
                df['encryption_key_index'] = max_id

                # Load address data
                df_address = ti.xcom_pull(key='Customer_Address_Table', task_ids='transform_data')
                if not df.empty:# Run only if the data frame is not empty    
                    success, nchunks, nrows, _ = write_pandas(conn, df_address, 'Customer_Address'.upper())
            
            # Create a Snowflake connection and insert the data
            success, nchunks, nrows, _ = write_pandas(conn, df, table['table'].upper())

    print('Sync Successful!')
    
    # End the connection
    cursor.close()
    conn.close()

# Function to update local JSON File
def update_json(ti):
   # Retrieve data from task instance with specific key
    table_data = ti.xcom_pull(key='json_data', task_ids='read_json_file') 
    for i in range(len(table_data)):
        df = ti.xcom_pull(key=table_data[i]['table'], task_ids='transform_data')
        if not df.empty:# Run only if the data frame is not empty
            table_data[i]['index'] =int(df[table_data[i]['primary_key']].max())

    # File directory
    filename = './primary_key_validator.json'

    # Overwrite file
    with open(filename, 'w') as json_file:  
            # Overwrite the file
            json.dump(table_data, json_file, indent=4)

    # Logging 
    print("JSON file updated successfully.")

validate_json_task = PythonOperator(
    task_id='validate_json_file',
    python_callable=validate_json_file,
    dag=dag
)

read_json_task = PythonOperator(
    task_id='read_json_file',
    python_callable=read_json_file,
    provide_context=True,
    dag=dag
)

extract_postgres_task = PythonOperator(
    task_id='extract_postgres_data',
    python_callable=extract_latest_data,
    provide_context=True,
    dag=dag
)

transform_data_task = PythonOperator(
    task_id='transform_data',
    python_callable=transform_data,
    provide_context=True,
    dag=dag
)

load_key_task = PythonOperator(
    task_id='load_key',
    python_callable=load_rotated_keys,
    provide_context=True,
    dag=dag
)

load_data_task = PythonOperator(
    task_id='load_data',
    python_callable=load_data,
    provide_context=True,
    dag=dag
)

update_json_task = PythonOperator(
    task_id='update_json',
    python_callable=update_json,
    provide_context=True,
    dag=dag
)

validate_json_task>>read_json_task>>extract_postgres_task>>transform_data_task>>load_key_task>>load_data_task>>update_json_task
