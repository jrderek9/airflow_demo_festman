# Apache Airflow CSV Automation - Troubleshooting Guide

## Common Issues and Solutions

### 1. DAG Import Errors

**Symptom**: DAG doesn't appear in Airflow UI

**Check**:
```bash
# Test the DAG file directly
docker exec airflow-csv-postgres-airflow-scheduler-1 python3 /opt/airflow/dags/your_dag.py

# Check Airflow import errors
docker-compose logs airflow-scheduler | grep -i "error"
```

**Common Fixes**:
- Remove `if __name__ == "__main__": dag.cli()` from DAG files
- Ensure all required directories exist
- Check file permissions

### 2. PostgreSQL Connection Issues

**Symptom**: Tasks fail with connection errors

**Check**:
```bash
# Test connection
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections get write_to_psql

# Test PostgreSQL directly
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT 1"
```

**Fix**:
```bash
# Re-create connection
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections delete write_to_psql
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections add write_to_psql \
    --conn-type postgres \
    --conn-host postgres \
    --conn-schema airflow \
    --conn-login airflow \
    --conn-password airflow \
    --conn-port 5432
```

### 3. Missing SQL Files

**Symptom**: `TemplateNotFound: sql/xxx_insert.sql`

**Fix**:
```bash
# Create SQL directory and files
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "
    mkdir -p /opt/airflow/dags/sql
    touch /opt/airflow/dags/sql/employees_insert.sql
    touch /opt/airflow/dags/sql/products_insert.sql
    touch /opt/airflow/dags/sql/sales_transactions_insert.sql
    touch /opt/airflow/dags/sql/insert_queries.sql
"
```

### 4. Permission Errors

**Symptom**: `Permission denied` when writing files

**Fix**:
```bash
# Fix permissions on host
sudo chown -R $(id -u):$(id -g) dags/ logs/ sample_files/

# Fix in container
docker exec -u root airflow-csv-postgres-airflow-scheduler-1 chown -R airflow:root /opt/airflow/dags
```

### 5. DAG Not Running

**Symptom**: DAG is visible but not executing

**Check**:
```bash
# Check if DAG is paused
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list | grep your_dag_id

# Check DAG state
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags state your_dag_id 2024-01-01
```

**Fix**:
```bash
# Unpause DAG
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause your_dag_id

# Trigger manually
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger your_dag_id
```

### 6. Large CSV Processing Issues

**Symptom**: Memory errors or slow processing

**Fix**:
- Increase Docker memory allocation
- Reduce chunk size in `csv_processor.py` (default is 10,000 rows)
- Process files sequentially instead of in parallel

### 7. Scheduler Not Picking Up Changes

**Symptom**: Modified DAGs don't update

**Fix**:
```bash
# Restart scheduler
docker-compose restart airflow-scheduler

# Force reparse
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags reserialize
```

### 8. Database Table Already Exists

**Symptom**: `ERROR: relation "table_name" already exists`

**Fix**:
- Add `DROP TABLE IF EXISTS` before CREATE TABLE
- Use `CREATE TABLE IF NOT EXISTS`
- Add `ON CONFLICT DO NOTHING` to INSERT statements

### 9. Deprecation Warnings

**Symptom**: Warnings about `schedule_interval`, `PostgresOperator`, etc.

**Note**: These are just warnings - DAGs will still work. To fix:
- Replace `schedule_interval` with `schedule`
- Replace `PostgresOperator` with `SQLExecuteQueryOperator`
- Remove `provide_context=True` (it's automatic now)

### 10. Container Won't Start

**Fix**:
```bash
# Clean restart
docker-compose down -v  # Warning: This removes volumes!
docker-compose up -d

# Check logs
docker-compose logs airflow-webserver
docker-compose logs airflow-scheduler
docker-compose logs postgres
```

## Useful Commands

### View Logs
```bash
# Real-time logs
docker-compose logs -f airflow-scheduler

# Specific container logs
docker logs airflow-csv-postgres-airflow-scheduler-1 --tail 100

# Task logs
# Go to Airflow UI → DAG → Task Instance → Log
```

### Database Queries
```bash
# Connect to PostgreSQL
docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow

# Useful queries
SELECT tablename, n_live_tup as row_count 
FROM pg_stat_user_tables 
ORDER BY n_live_tup DESC;

SELECT pg_size_pretty(pg_database_size('airflow'));
```

### Reset Everything
```bash
# Complete reset (WARNING: Deletes all data!)
docker-compose down -v
rm -rf logs/*
docker-compose up -d
```

## Getting Help

1. Check Airflow UI "Browse" → "Task Instances" for detailed error messages
2. Enable debug logging: Set `AIRFLOW__LOGGING__LOGGING_LEVEL=DEBUG` in docker-compose.yml
3. Check the official Airflow documentation: https://airflow.apache.org/docs/