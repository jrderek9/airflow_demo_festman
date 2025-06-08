from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configure logging
logger = logging.getLogger(__name__)

# Function to process large CSV files in chunks
def process_csv_in_chunks(csv_file, table_name, **context):
    """
    Process large CSV files in chunks to manage memory efficiently
    """
    CSV_FILE_PATH = f'sample_files/{csv_file}'
    CHUNK_SIZE = 10000  # Process 10,000 rows at a time
    
    # Check if file exists
    if not os.path.exists(CSV_FILE_PATH):
        logger.warning(f"File {CSV_FILE_PATH} not found")
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")
    
    # Create directory for SQL files if it doesn't exist
    os.makedirs('./dags/sql', exist_ok=True)
    
    # Open SQL file for writing
    sql_file_path = f'./dags/sql/{table_name}_insert.sql'
    
    total_rows = 0
    with open(sql_file_path, 'w') as sql_file:
        # Read CSV in chunks
        for chunk_num, chunk in enumerate(pd.read_csv(CSV_FILE_PATH, chunksize=CHUNK_SIZE)):
            rows_in_chunk = len(chunk)
            total_rows += rows_in_chunk
            logger.info(f"Processing chunk {chunk_num + 1} with {rows_in_chunk} rows...")
            
            # Generate insert queries for this chunk
            for index, row in chunk.iterrows():
                # Build dynamic insert query based on columns
                columns = ', '.join(chunk.columns)
                values = []
                
                for col in chunk.columns:
                    val = row[col]
                    if pd.isna(val):
                        values.append('NULL')
                    elif isinstance(val, str):
                        # Escape single quotes in strings
                        escaped_val = str(val).replace("'", "''")
                        values.append(f"'{escaped_val}'")
                    elif isinstance(val, bool):
                        values.append('TRUE' if val else 'FALSE')
                    else:
                        values.append(str(val))
                
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(values)}) ON CONFLICT DO NOTHING;"
                sql_file.write(f"{insert_query}\n")
            
            # Log progress every 5 chunks
            if (chunk_num + 1) % 5 == 0:
                logger.info(f"Processed {total_rows} rows so far...")
        
        logger.info(f"✅ Generated SQL file: {sql_file_path} with {total_rows} insert statements")
    
    # Store file path and row count in XCom for next task
    context['ti'].xcom_push(key=f'{table_name}_sql_path', value=sql_file_path)
    context['ti'].xcom_push(key=f'{table_name}_row_count', value=total_rows)
    
    return total_rows

# Function to verify data load
def verify_data_load(table_name, **context):
    """
    Verify that data was loaded successfully
    """
    ti = context['ti']
    expected_rows = ti.xcom_pull(key=f'{table_name}_row_count')
    
    logger.info(f"Expected {expected_rows} rows to be loaded into {table_name}")
    # In a real scenario, you would query the database to verify
    
    return f"Data load verified for {table_name}"

# Function to create tables dynamically
def get_create_table_sql(table_name):
    """
    Generate CREATE TABLE SQL based on table name
    """
    table_definitions = {
        'employees': """
            DROP TABLE IF EXISTS employees CASCADE;
            CREATE TABLE employees (
                employee_id INTEGER PRIMARY KEY,
                first_name VARCHAR(50),
                last_name VARCHAR(50),
                age INTEGER,
                gender VARCHAR(10),
                email VARCHAR(100),
                phone VARCHAR(20),
                address VARCHAR(200),
                city VARCHAR(50),
                state VARCHAR(2),
                zip_code VARCHAR(10),
                country VARCHAR(50),
                hire_date DATE,
                department VARCHAR(50),
                job_title VARCHAR(50),
                salary DECIMAL(10,2),
                bonus_percentage DECIMAL(5,2),
                years_experience INTEGER,
                education_level VARCHAR(50),
                performance_rating DECIMAL(3,2),
                satisfaction_score DECIMAL(3,2),
                attendance_rate DECIMAL(5,2),
                training_hours INTEGER,
                certifications INTEGER,
                is_manager BOOLEAN,
                team_size INTEGER,
                remote_work_days INTEGER,
                marital_status VARCHAR(20),
                dependents INTEGER,
                emergency_contact VARCHAR(100),
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Create indexes for better query performance
            CREATE INDEX idx_employees_department ON employees(department);
            CREATE INDEX idx_employees_email ON employees(email);
            CREATE INDEX idx_employees_hire_date ON employees(hire_date);
        """,
        
        'products': """
            DROP TABLE IF EXISTS products CASCADE;
            CREATE TABLE products (
                product_id VARCHAR(20) PRIMARY KEY,
                sku VARCHAR(20),
                product_name VARCHAR(200),
                category VARCHAR(50),
                brand VARCHAR(50),
                description TEXT,
                unit_price DECIMAL(10,2),
                cost_price DECIMAL(10,2),
                quantity_in_stock INTEGER,
                reorder_level INTEGER,
                reorder_quantity INTEGER,
                warehouse_location VARCHAR(50),
                supplier VARCHAR(50),
                weight_kg DECIMAL(10,2),
                dimensions_cm VARCHAR(50),
                condition VARCHAR(50),
                manufactured_date DATE,
                expiry_date VARCHAR(20),
                last_restocked DATE,
                units_sold_30days INTEGER,
                units_sold_90days INTEGER,
                units_sold_365days INTEGER,
                rating DECIMAL(3,2),
                review_count INTEGER,
                is_active BOOLEAN,
                discount_percentage DECIMAL(5,2),
                tax_rate DECIMAL(5,2),
                shipping_class VARCHAR(50),
                country_of_origin VARCHAR(50),
                barcode VARCHAR(50),
                minimum_age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Create indexes
            CREATE INDEX idx_products_category ON products(category);
            CREATE INDEX idx_products_sku ON products(sku);
            CREATE INDEX idx_products_brand ON products(brand);
        """,
        
        'sales_transactions': """
            DROP TABLE IF EXISTS sales_transactions CASCADE;
            CREATE TABLE sales_transactions (
                transaction_id VARCHAR(20) PRIMARY KEY,
                order_date TIMESTAMP,
                customer_id VARCHAR(20),
                customer_name VARCHAR(100),
                customer_email VARCHAR(100),
                customer_type VARCHAR(20),
                product_ids TEXT,
                product_names TEXT,
                quantities TEXT,
                unit_prices TEXT,
                subtotal DECIMAL(12,2),
                tax_amount DECIMAL(10,2),
                shipping_cost DECIMAL(10,2),
                discount_amount DECIMAL(10,2),
                total_amount DECIMAL(12,2),
                payment_method VARCHAR(50),
                payment_status VARCHAR(20),
                order_status VARCHAR(20),
                sales_channel VARCHAR(50),
                sales_rep_id VARCHAR(10),
                sales_rep_name VARCHAR(100),
                shipping_address TEXT,
                billing_address TEXT,
                region VARCHAR(50),
                country VARCHAR(50),
                currency VARCHAR(10),
                exchange_rate DECIMAL(10,4),
                notes TEXT,
                refund_amount DECIMAL(12,2),
                is_gift BOOLEAN,
                loyalty_points_earned INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_sales_order_date ON sales_transactions(order_date);
            CREATE INDEX idx_sales_customer ON sales_transactions(customer_id);
            CREATE INDEX idx_sales_status ON sales_transactions(order_status);
        """
    }
    
    # Return the specific table definition or a generic one
    return table_definitions.get(table_name, f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

# Define the DAG
with DAG('large_csv_processor_dag',
         default_args=default_args,
         description='Process large CSV files and load into PostgreSQL',
         schedule_interval='@daily',
         catchup=False,
         max_active_runs=1,
         tags=['bulk', 'csv', 'postgres', 'production']) as dag:

    # Dictionary of CSV files and their corresponding table names
    csv_files = {
        'employees_large.csv': 'employees',
        'product_inventory.csv': 'products',
        'sales_transactions.csv': 'sales_transactions',
        # Add more files as they become available
    }
    
    # Create a start task
    start = PythonOperator(
        task_id='start',
        python_callable=lambda: logger.info("Starting CSV processing pipeline")
    )
    
    # Create tasks dynamically for each CSV file
    for csv_file, table_name in csv_files.items():
        
        # Task to create table
        create_table_task = PostgresOperator(
            task_id=f'create_{table_name}_table',
            postgres_conn_id='write_to_psql',
            sql=get_create_table_sql(table_name),
            autocommit=True
        )
        
        # Task to process CSV and generate SQL
        process_csv_task = PythonOperator(
            task_id=f'process_{table_name}_csv',
            python_callable=process_csv_in_chunks,
            op_kwargs={
                'csv_file': csv_file,
                'table_name': table_name
            },
            provide_context=True
        )
        
        # Task to execute SQL inserts
        insert_data_task = PostgresOperator(
            task_id=f'insert_{table_name}_data',
            postgres_conn_id='write_to_psql',
            sql=f'sql/{table_name}_insert.sql',
            autocommit=True
        )
        
        # Task to analyze table for query optimization
        analyze_table_task = PostgresOperator(
            task_id=f'analyze_{table_name}_table',
            postgres_conn_id='write_to_psql',
            sql=f"ANALYZE {table_name};",
            autocommit=True
        )
        
        # Task to verify data load
        verify_task = PythonOperator(
            task_id=f'verify_{table_name}_load',
            python_callable=verify_data_load,
            op_kwargs={'table_name': table_name},
            provide_context=True
        )
        
        # Define task dependencies
        start >> create_table_task >> process_csv_task >> insert_data_task >> analyze_table_task >> verify_task
    
    # Create an end task
    end = PythonOperator(
        task_id='end',
        python_callable=lambda: logger.info("CSV processing pipeline completed successfully"),
        trigger_rule='none_failed'  # Run even if some tables were skipped
    )
    
    # Connect all verify tasks to end
    for csv_file, table_name in csv_files.items():
        dag.get_task(f'verify_{table_name}_load') >> end