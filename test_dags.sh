#!/bin/bash

echo "🧪 Testing Airflow DAGs"
echo "====================="
echo ""

# Function to trigger and check DAG
test_dag() {
    local dag_id=$1
    local description=$2
    
    echo "Testing: $dag_id"
    echo "Description: $description"
    echo "-----------------------------------"
    
    # Trigger the DAG
    echo "Triggering DAG..."
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger "$dag_id"
    
    # Wait a moment
    sleep 3
    
    # Check the last run
    echo "Checking status..."
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list-runs -d "$dag_id" --limit 1
    
    echo ""
}

# 1. Test simple DAG first
test_dag "simple_test_dag" "Basic test DAG - should print Hello World"

# 2. Test simple CSV to PostgreSQL
echo "Preparing CSV file for simple_csv_to_postgres_dag..."
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "
    mkdir -p /opt/airflow/sample_files
    cat > /opt/airflow/sample_files/input.csv << 'CSV'
id,name,age
1,John Doe,30
2,Jane Smith,25
3,Bob Johnson,35
4,Alice Williams,28
5,Charlie Brown,32
CSV
"
test_dag "simple_csv_to_postgres_dag" "Load CSV to PostgreSQL"

# 3. Check table creation
echo "Verifying data in PostgreSQL..."
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "
    SELECT 'Tables created by DAGs:' as info;
    SELECT tablename FROM pg_tables WHERE schemaname = 'public' AND tablename = 'sample_table';
    SELECT 'Row count in sample_table:' as info, COUNT(*) as rows FROM sample_table;
"

echo ""
echo "📋 View detailed logs in Airflow UI:"
echo "   http://localhost:8080"
echo "   Username: admin"
echo "   Password: admin"
echo ""
echo "Click on the DAG → Graph View → Click on a task → View Log"