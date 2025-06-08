#!/bin/bash

# Verification script to ensure everything is working
echo "🔍 VERIFYING AIRFLOW SETUP"
echo "=========================="
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Check 1: Services running
echo "1️⃣ Checking services..."
services=("scheduler" "webserver" "postgres")
all_good=true

for service in "${services[@]}"; do
    if docker ps | grep -q "airflow-csv-postgres-airflow-${service}-1"; then
        echo -e "  ${GREEN}✅ ${service} is running${NC}"
    else
        echo -e "  ${RED}❌ ${service} is NOT running${NC}"
        all_good=false
    fi
done

# Check 2: Database connection
echo -e "\n2️⃣ Checking database connection..."
if docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT 1;" &>/dev/null; then
    echo -e "  ${GREEN}✅ Database connection OK${NC}"
else
    echo -e "  ${RED}❌ Database connection FAILED${NC}"
    all_good=false
fi

# Check 3: Sample files
echo -e "\n3️⃣ Checking sample files..."
file_count=$(docker exec airflow-csv-postgres-airflow-scheduler-1 ls /opt/airflow/sample_files/*.csv 2>/dev/null | wc -l)
if [ "$file_count" -ge 5 ]; then
    echo -e "  ${GREEN}✅ Found $file_count sample CSV files${NC}"
    docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "ls -lh /opt/airflow/sample_files/*.csv | awk '{print \"    \" \$9 \": \" \$5}'"
else
    echo -e "  ${RED}❌ Only $file_count sample files found (expected 6+)${NC}"
    all_good=false
fi

# Check 4: DAGs
echo -e "\n4️⃣ Checking DAGs..."
dag_count=$(docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list 2>/dev/null | grep -c "airflow")
if [ "$dag_count" -ge 5 ]; then
    echo -e "  ${GREEN}✅ Found $dag_count DAGs${NC}"
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list | grep -E "(simple_csv|incremental|monitoring|large_csv)" | while read line; do
        echo "    $line"
    done
else
    echo -e "  ${RED}❌ Only $dag_count DAGs found${NC}"
    all_good=false
fi

# Check 5: Test simple DAG
echo -e "\n5️⃣ Testing simple DAG execution..."
echo "  Triggering simple_csv_to_postgres_dag..."
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_csv_to_postgres_dag &>/dev/null

# Wait a bit for execution
sleep 5

# Check if sample_table was created
if docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT COUNT(*) FROM sample_table;" &>/dev/null; then
    row_count=$(docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM sample_table;" | tr -d ' ')
    if [ "$row_count" -gt 0 ]; then
        echo -e "  ${GREEN}✅ sample_table created with $row_count rows${NC}"
    else
        echo -e "  ${YELLOW}⚠️  sample_table created but empty${NC}"
    fi
else
    echo -e "  ${YELLOW}⚠️  sample_table not created yet (DAG may still be running)${NC}"
fi

# Check 6: Web UI
echo -e "\n6️⃣ Checking Airflow Web UI..."
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 | grep -q "200\|302"; then
    echo -e "  ${GREEN}✅ Web UI accessible at http://localhost:8080${NC}"
else
    echo -e "  ${RED}❌ Web UI not accessible${NC}"
    all_good=false
fi

# Summary
echo -e "\n📊 SUMMARY"
echo "=========="
if $all_good; then
    echo -e "${GREEN}✅ Everything is working correctly!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Open http://localhost:8080 (admin/admin)"
    echo "2. Run ./run_dags.sh for interactive menu"
    echo "3. Or trigger DAGs manually:"
    echo "   docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_csv_to_postgres_dag"
    echo "   docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger large_csv_processor_dag"
else
    echo -e "${RED}❌ Some issues detected${NC}"
    echo ""
    echo "Try running:"
    echo "  ./fix_all_issues_v2.sh"
    echo ""
    echo "Or restart services:"
    echo "  docker-compose restart"
fi

echo ""
echo "📝 Useful commands:"
echo "  View logs: docker-compose logs -f"
echo "  List tables: docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c '\\dt'"
echo "  Monitor DAGs: ./monitor_dags.sh"