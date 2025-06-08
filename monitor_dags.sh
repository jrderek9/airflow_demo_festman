#!/bin/bash

# Simple DAG monitoring script
echo "📊 DAG MONITORING"
echo "================"
echo ""

# Check running DAGs
echo "🔄 Running DAGs:"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list-runs --state running

echo ""
echo "✅ Successful runs (last 5):"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list-runs --state success --limit 5

echo ""
echo "❌ Failed runs (if any):"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list-runs --state failed --limit 5

echo ""
echo "📊 Database tables:"
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "\dt" 2>/dev/null

echo ""
echo "📈 Row counts:"
tables=("sample_table" "employees" "products" "sales_transactions" "support_tickets" "web_analytics" "healthcare_records")
for table in "${tables[@]}"; do
    count=$(docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ')
    if [ -n "$count" ] && [ "$count" != "" ]; then
        printf "  %-25s: %s rows\n" "$table" "$count"
    fi
done
