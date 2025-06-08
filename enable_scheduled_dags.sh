#!/bin/bash

echo "⏰ Enable Scheduled DAGs"
echo "======================"
echo ""

# Function to unpause a DAG
unpause_dag() {
    local dag_id=$1
    local schedule=$2
    
    echo "Unpausing: $dag_id (Schedule: $schedule)"
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause "$dag_id"
    echo "✅ $dag_id is now active"
    echo ""
}

# Show current DAG states
echo "Current DAG States:"
echo "------------------"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list-runs --limit 1

echo ""
echo "Enabling Scheduled DAGs:"
echo "----------------------"

# Enable incremental processor (runs hourly)
unpause_dag "incremental_data_processor" "@hourly"

# Enable monitoring DAG (runs every 6 hours)
unpause_dag "data_monitoring_and_reporting" "0 */6 * * *"

# The simple DAGs are meant to be triggered manually, so we'll leave them paused

echo "📊 Scheduled DAGs Status:"
echo "-----------------------"
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "
    airflow dags list | grep -E '(incremental_data_processor|data_monitoring_and_reporting)'
"

echo ""
echo "⏳ These DAGs will run automatically:"
echo "  - incremental_data_processor: Every hour (generates new data)"
echo "  - data_monitoring_and_reporting: Every 6 hours (creates reports)"
echo ""
echo "📁 Check generated files:"
echo "  - Incremental data: /opt/airflow/dags/incremental_data/"
echo "  - Reports: /opt/airflow/dags/reports/"
echo ""
echo "To pause them again:"
echo "  docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags pause <dag_id>"