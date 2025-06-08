from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# Function to read the CSV and generate insert queries
def generate_insert_queries():
    """
    Read CSV file and generate SQL insert statements
    """
    CSV_FILE_PATH = 'sample_files/input.csv'
    
    # Check if file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Warning: {CSV_FILE_PATH} not found. Creating sample data...")
        # Create sample data if file doesn't exist
        sample_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Williams', 'Charlie Brown'],
            'age': [30, 25, 35, 28, 32]
        }
        os.makedirs('sample_files', exist_ok=True)
        pd.DataFrame(sample_data).to_csv(CSV_FILE_PATH, index=False)
    
    # Read the CSV file
    df = pd.read_csv(CSV_FILE_PATH)
    print(f"Read {len(df)} rows from {CSV_FILE_PATH}")
    
    # Create directory for SQL files if it doesn't exist
    os.makedirs('./dags/sql', exist_ok=True)
    
    # Create a list of SQL insert queries
    insert_queries = []
    for index, row in df.iterrows():
        # Escape single quotes in names
        name = str(row['name']).replace("'", "''")
        insert_query = f"INSERT INTO sample_table (id, name, age) VALUES ({row['id']}, '{name}', {row['age']});"
        insert_queries.append(insert_query)
    
    # Save queries to a file for the PostgresOperator to execute
    sql_file_path = './dags/sql/insert_queries.sql'
    with open(sql_file_path, 'w') as f:
        for query in insert_queries:
            f.write(f"{query}\n")
    
    print(f"Generated {len(insert_queries)} insert queries in {sql_file_path}")
    return len(insert_queries)

# Define the DAG
with DAG('simple_csv_to_postgres_dag',
         default_args=default_args,
         description='Simple DAG to load CSV data into PostgreSQL',
         schedule_interval='@once',
         catchup=False,
         tags=['simple', 'csv', 'postgres']) as dag:

    # Task to create a PostgreSQL table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='write_to_psql',
        sql="""
        DROP TABLE IF EXISTS sample_table;
        CREATE TABLE sample_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            age INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_sample_name ON sample_table(name);
        """,
        autocommit=True
    )

    # Task to generate insert queries from CSV
    generate_queries = PythonOperator(
        task_id='generate_insert_queries',
        python_callable=generate_insert_queries
    )

    # Task to run the generated SQL queries
    run_insert_queries = PostgresOperator(
        task_id='run_insert_queries',
        postgres_conn_id='write_to_psql',
        sql='sql/insert_queries.sql',
        autocommit=True
    )
    
    # Task to verify data was inserted
    verify_data = PostgresOperator(
        task_id='verify_data',
        postgres_conn_id='write_to_psql',
        sql="""
        SELECT COUNT(*) as total_records, 
               MIN(age) as min_age, 
               MAX(age) as max_age,
               AVG(age) as avg_age
        FROM sample_table;
        """,
        autocommit=True
    )

    # Define task dependencies
    create_table >> generate_queries >> run_insert_queries >> verify_data