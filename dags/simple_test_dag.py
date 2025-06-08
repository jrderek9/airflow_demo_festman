from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")
    return "Hello World"

def print_date(**context):
    print(f"Execution date: {context['ds']}")
    print(f"Current time: {datetime.now()}")
    return "Date printed successfully"

with DAG(
    'simple_test_dag',
    default_args=default_args,
    description='A simple test DAG to verify Airflow is working',
    schedule_interval='@once',
    catchup=False,
    tags=['test', 'example']
) as dag:
    
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )
    
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
        provide_context=True
    )
    
    # Set task dependencies
    hello_task >> date_task