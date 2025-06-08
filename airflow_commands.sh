#!/bin/bash

# Helper script with correct container names

echo "🎯 AIRFLOW HELPER COMMANDS"
echo "========================="
echo ""
echo "Container Names:"
echo "  Scheduler: airflow-csv-postgres-airflow-scheduler-1"
echo "  Webserver: airflow-csv-postgres-airflow-webserver-1"
echo "  PostgreSQL: airflow-csv-postgres-postgres-1"
echo ""
echo "Useful Commands:"
echo ""
echo "# List all DAGs:"
echo "docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list"
echo ""
echo "# Trigger a DAG:"
echo "docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger <dag_id>"
echo ""
echo "# Pause/Unpause a DAG:"
echo "docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags pause <dag_id>"
echo "docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause <dag_id>"
echo ""
echo "# Check DAG status:"
echo "docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags state <dag_id> <execution_date>"
echo ""
echo "# View logs:"
echo "docker logs airflow-csv-postgres-airflow-scheduler-1"
echo ""
echo "# Access PostgreSQL:"
echo "docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow"
echo ""
echo "# Check table data:"
echo 'docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM employees;"'
echo ""

# Function to trigger all DAGs
trigger_all() {
    echo "Triggering all DAGs..."
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_csv_to_postgres_dag
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger large_csv_processor_dag
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause incremental_data_processor
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause data_monitoring_and_reporting
    echo "All DAGs triggered!"
}

# Function to check data in all tables
check_data() {
    echo "Checking data in all tables..."
    tables=("sample_table" "employees" "products" "sales_transactions" "support_tickets" "web_analytics" "healthcare_records")
    
    for table in "${tables[@]}"; do
        count=$(docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null || echo "0")
        if [ "$count" != "0" ]; then
            echo "  $table: $count rows"
        fi
    done
}

# Check if function was called
case "$1" in
    "trigger-all")
        trigger_all
        ;;
    "check-data")
        check_data
        ;;
    *)
        # Just show the help
        ;;
esac
