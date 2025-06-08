# Apache Airflow CSV Automation - DAG Usage Guide

## Overview of Available DAGs

Your project includes 5 different DAGs, each serving a specific purpose:

### 1. **simple_test_dag** ✅
- **Purpose**: Basic test to verify Airflow is working
- **Schedule**: `@once` (manual trigger only)
- **What it does**: Prints "Hello from Airflow!" and the current date
- **Use case**: Testing your Airflow installation

**How to run:**
```bash
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_test_dag
```

### 2. **simple_csv_to_postgres_dag** ✅
- **Purpose**: Load data from a small CSV file into PostgreSQL
- **Schedule**: `@once` (manual trigger only)
- **What it does**: 
  - Creates a `sample_table`
  - Reads `input.csv` (5 rows)
  - Inserts data into PostgreSQL
  - Verifies the data load
- **Use case**: Quick testing of CSV to PostgreSQL pipeline

**How to run:**
```bash
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_csv_to_postgres_dag
```

### 3. **large_csv_processor_dag** 
- **Purpose**: Process large CSV files in chunks
- **Schedule**: `@daily`
- **What it does**:
  - Processes multiple large CSV files
  - Handles millions of rows efficiently
  - Creates tables dynamically
  - Uses chunked processing (10,000 rows at a time)
- **Use case**: Production-scale data loading

**How to run:**
```bash
# First ensure you have the large CSV files
ls sample_files/

# Trigger the DAG
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger large_csv_processor_dag
```

### 4. **incremental_data_processor** 
- **Purpose**: Continuously load new data every hour
- **Schedule**: `@hourly`
- **What it does**:
  - Generates new incremental data
  - Tracks last processed ID
  - Loads only new records
  - Maintains 6 tables with 30+ columns each
- **Use case**: Real-time data ingestion

**How to enable:**
```bash
# Unpause the DAG to start hourly runs
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause incremental_data_processor
```

### 5. **data_monitoring_and_reporting**
- **Purpose**: Monitor data quality and generate reports
- **Schedule**: Every 6 hours
- **What it does**:
  - Runs data quality checks
  - Generates executive summaries
  - Creates monitoring reports
  - Detects anomalies
  - Archives old reports
- **Use case**: Data governance and quality assurance

**How to enable:**
```bash
# Unpause the DAG
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause data_monitoring_and_reporting

# Check reports
docker exec airflow-csv-postgres-airflow-scheduler-1 ls -la /opt/airflow/dags/reports/
```

## Common Operations

### Check DAG Status
```bash
# List all DAGs
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list

# Check DAG state
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags state <dag_id> <execution_date>
```

### View Task Logs
```bash
# In the Airflow UI: http://localhost:8080
# Click on DAG → Graph View → Click on task → View Log
```

### Manual DAG Trigger with Parameters
```bash
# Trigger with configuration
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger <dag_id> \
    --conf '{"param1": "value1", "param2": "value2"}'
```

### Database Operations
```bash
# Connect to PostgreSQL
docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow

# Common queries
\dt                                    # List all tables
\d+ table_name                        # Describe table structure
SELECT COUNT(*) FROM table_name;      # Count rows
SELECT * FROM table_name LIMIT 10;    # Preview data
```

## Troubleshooting

### DAG Not Appearing
1. Check for import errors:
   ```bash
   docker exec airflow-csv-postgres-airflow-scheduler-1 python3 /opt/airflow/dags/your_dag.py
   ```

2. Check Airflow logs:
   ```bash
   docker-compose logs airflow-scheduler | grep ERROR
   ```

3. Verify file permissions:
   ```bash
   docker exec airflow-csv-postgres-airflow-scheduler-1 ls -la /opt/airflow/dags/
   ```

### Task Failures
1. Check task logs in Airflow UI
2. Verify database connection:
   ```bash
   docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections get write_to_psql
   ```

3. Test connection manually:
   ```bash
   docker exec airflow-csv-postgres-airflow-scheduler-1 python3 -c "
   from airflow.providers.postgres.hooks.postgres import PostgresHook
   hook = PostgresHook(postgres_conn_id='write_to_psql')
   conn = hook.get_conn()
   print('Connection successful!')
   "
   ```

## Performance Tips

1. **For Large Files**: Use `large_csv_processor_dag` with chunked processing
2. **For Continuous Data**: Use `incremental_data_processor` to avoid reprocessing
3. **Monitor Performance**: Check the monitoring reports for bottlenecks
4. **Optimize Queries**: Add indexes on frequently queried columns

## Best Practices

1. **Always test with small data first** using `simple_csv_to_postgres_dag`
2. **Monitor data quality** using the monitoring DAG
3. **Use incremental loading** for continuously growing datasets
4. **Set appropriate retry policies** for production DAGs
5. **Archive old data** to maintain performance