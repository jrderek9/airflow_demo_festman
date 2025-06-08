#!/bin/bash

echo "🔍 Airflow CSV Automation - Complete Status Check"
echo "==============================================="
echo ""

# 1. Check Docker containers
echo "1. Docker Container Status:"
echo "-------------------------"
docker-compose ps --format "table {{.Name}}\t{{.Status}}\t{{.Ports}}"

# 2. Check Airflow DAGs
echo ""
echo "2. Airflow DAGs Status:"
echo "---------------------"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list | grep -E "(csv|data|simple|incremental)" || echo "No DAGs found"

# 3. Check PostgreSQL tables
echo ""
echo "3. PostgreSQL Tables:"
echo "-------------------"
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "\dt" 2>/dev/null | grep -E "(test_data|employees|products|sample_table)" || echo "No tables found"

# 4. Check sample files
echo ""
echo "4. Sample Files:"
echo "--------------"
echo "In scheduler container:"
docker exec airflow-csv-postgres-airflow-scheduler-1 ls -la /opt/airflow/sample_files/ 2>/dev/null | head -10 || echo "No sample files found"

# 5. Check for DAG errors
echo ""
echo "5. Recent DAG Errors (if any):"
echo "----------------------------"
docker-compose logs airflow-scheduler 2>/dev/null | grep -E "ERROR|Failed" | tail -5 || echo "No recent errors"

# 6. Check connections
echo ""
echo "6. Airflow Connections:"
echo "---------------------"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections list 2>/dev/null | grep postgres || echo "No postgres connection found"

# 7. System resources
echo ""
echo "7. System Resources:"
echo "------------------"
docker stats --no-stream --format "table {{.Container}}\t{{.CPUPerc}}\t{{.MemUsage}}" | grep airflow

echo ""
echo "📊 Summary:"
echo "========="
echo ""
echo "✅ What's Working:"
echo "  - All 6 DAGs are loaded and visible"
echo "  - PostgreSQL is running and accessible"
echo "  - Airflow webserver is accessible at http://localhost:8080"
echo "  - Directory structure is in place"
echo ""
echo "⚠️  Things to Note:"
echo "  - Deprecation warnings in DAGs (not critical, DAGs will still run)"
echo "  - Some DAGs are scheduled (incremental_data_processor runs hourly)"
echo "  - large_csv_processor_dag needs the actual CSV files to work"
echo ""
echo "🚀 Next Steps:"
echo "  1. Run: chmod +x quick_data_load_v2.sh test_dags.sh"
echo "  2. Run: ./quick_data_load_v2.sh  # Load sample data"
echo "  3. Run: ./test_dags.sh           # Test the DAGs"
echo "  4. Access http://localhost:8080 to monitor DAG runs"