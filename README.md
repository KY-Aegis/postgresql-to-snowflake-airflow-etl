# PostgreSQL to Snowflake ETL with Airflow

This project demonstrates an ETL (Extract, Transform, Load) pipeline using Apache Airflow to transfer data from a PostgreSQL database to a Snowflake data warehouse. It also includes daily aggregation for marketing analysis.

## Project Overview

* **Data Source:** PostgreSQL (simulated as an AWS RDS instance)
* **Data Destination:** Snowflake
* **ETL Tool:** Apache Airflow
* **Purpose:**
    * Load all data from PostgreSQL to Snowflake for data retention.
    * Provide daily aggregated sales and order data for the marketing team.
    * Prepare data for visualization in tools like Tableau or Power BI.

## Key Features

* **Hourly Data Synchronization:** Airflow DAG runs hourly to sync data between PostgreSQL and Snowflake.
* **Daily Aggregation:** Daily aggregated data (sales, orders) based on timestamp, location, and product is generated for marketing analysis.
* **Scalability:** Designed to handle future surges in data volume.
* **Optimization:** Queries are optimized to minimize data transfer costs from AWS RDS.
* **Data Integrity:** Constraints are implemented in PostgreSQL to ensure data quality (e.g., preventing null values).
* **Historical Data Archiving**: Stored procedures and PgCron are used to archive historical data in PostgreSQL, improving query performance.
* **Symmetric Encryption**: the script utilizes symmetric encryption to encrypt senstive data before loading into snowflake, and the decryption key is stored in a seperate table with limited access.

## Project Setup

1.  **Prerequisites:**
    * Apache Airflow installed and configured.
    * PostgreSQL database with sample data.
    * Snowflake account and database.
    * python with pip installed.

2.  **Installation:**
    * Install dependencies:
        ```bash
        pip install -r requirements.txt
        ```
    * Configure environment variables:
        * Create a `.env` file with your PostgreSQL and Snowflake credentials.
        * Example `.env` file:
            ```bash
            POSTGRES_USER=your_postgres_user
            POSTGRES_PASSWORD=your_postgres_password
            POSTGRES_HOST=your_postgres_host
            POSTGRES_PORT=your_postgres_port
            POSTGRES_DB=your_postgres_db
            SNOWFLAKE_USER=your_snowflake_user
            SNOWFLAKE_PASSWORD=your_snowflake_password
            SNOWFLAKE_ACCOUNT=your_snowflake_account
            SNOWFLAKE_WAREHOUSE=your_snowflake_warehouse
            SNOWFLAKE_DATABASE=your_snowflake_database
            ```
3.  **Airflow DAG:**
    * Place the DAG file in your Airflow DAGs folder.
    * The DAG will run hourly for data synchronization and daily for aggregation.

4.  **Running the Pipeline:**
    * Trigger the DAG in the Airflow UI.

## Data Optimization

* **PostgreSQL Optimization:**
    * Optimized queries to reduce data transfer costs from AWS RDS.
    * Constraints to ensure data quality.
    * Archiving historical data to improve query performance.
* **Snowflake Optimization:**
    * Symmetric encryption for sensitive data.
    * Data aggregation for efficient marketing analysis.
