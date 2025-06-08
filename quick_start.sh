#!/bin/bash

# Quick Start Script - Gets everything running in one command
echo "🚀 AIRFLOW QUICK START"
echo "====================="
echo ""

# Check if already running
if docker ps | grep -q "airflow-csv-postgres"; then
    echo "✅ Airflow is already running!"
    echo ""
    echo "What would you like to do?"
    echo "1) Run simple CSV test (5 rows)"
    echo "2) Process all large CSV files (875,000 rows)"
    echo "3) View DAG status"
    echo "4) Open Airflow UI"
    echo "5) Check table data"
    echo ""
    read -p "Enter choice (1-5): " choice
    
    case $choice in
        1)
            echo "Running simple CSV test..."
            docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_csv_to_postgres_dag
            echo "✅ Triggered! Check progress at http://localhost:8080"
            ;;
        2)
            echo "Processing large CSV files..."
            docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger large_csv_processor_dag
            echo "✅ Started! This will take several minutes."
            ;;
        3)
            echo "DAG Status:"
            docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list-runs --limit 10
            ;;
        4)
            echo "Opening Airflow UI..."
            open http://localhost:8080 2>/dev/null || xdg-open http://localhost:8080 2>/dev/null || echo "Open http://localhost:8080 in your browser (admin/admin)"
            ;;
        5)
            echo "Table Data:"
            tables=("sample_table" "employees" "products" "sales_transactions")
            for table in "${tables[@]}"; do
                count=$(docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ')
                if [ -n "$count" ] && [ "$count" != "" ]; then
                    echo "  $table: $count rows"
                fi
            done
            ;;
    esac
else
    echo "Airflow is not running. Starting setup..."
    echo ""
    
    # Run the fix script
    if [ -f "fix_all_issues_v2.sh" ]; then
        ./fix_all_issues_v2.sh
    else
        echo "Error: fix_all_issues_v2.sh not found!"
        echo "Please run these commands first:"
        echo "  1. Save all the scripts I provided"
        echo "  2. chmod +x *.sh"
        echo "  3. ./quick_start.sh"
    fi
fi

echo ""
echo "📚 Documentation:"
echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
echo "  - pgAdmin: http://localhost:5050 (admin@pgadmin.com/admin)"
echo "  - Interactive menu: ./run_dags.sh"
echo "  - Monitor status: ./monitor_dags.sh"