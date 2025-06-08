#!/bin/bash

# Script to check for DAG import errors
echo "🔍 Checking DAG Import Errors"
echo "============================"
echo ""

# Check each DAG file for syntax/import errors
echo "Checking all DAG files..."
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c '
cd /opt/airflow/dags
for dag_file in *.py; do
    echo ""
    echo "Testing: $dag_file"
    echo "-------------------"
    python3 "$dag_file" 2>&1 | head -20
    if [ $? -eq 0 ]; then
        echo "✅ $dag_file - OK"
    else
        echo "❌ $dag_file - FAILED"
    fi
done
'

echo ""
echo "📋 Listing DAGs recognized by Airflow:"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list

echo ""
echo "📂 Checking for required directories:"
docker exec airflow-csv-postgres-airflow-scheduler-1 ls -la /opt/airflow/dags/