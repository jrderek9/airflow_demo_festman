#!/bin/bash

# Troubleshooting Script for Airflow CSV Automation
# This script diagnoses and fixes common issues

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

echo -e "${YELLOW}🔧 AIRFLOW TROUBLESHOOTING SCRIPT${NC}"
echo "================================="
echo ""

# Check 1: Docker containers
echo -e "${YELLOW}1. Checking Docker containers...${NC}"
if docker-compose ps | grep -q "airflow-csv-postgres-airflow-scheduler-1.*Up"; then
    echo -e "${GREEN}✅ Scheduler is running${NC}"
else
    echo -e "${RED}❌ Scheduler is not running${NC}"
    echo "   Fix: Run 'docker-compose up -d'"
fi

if docker-compose ps | grep -q "airflow-csv-postgres-airflow-webserver-1.*Up"; then
    echo -e "${GREEN}✅ Webserver is running${NC}"
else
    echo -e "${RED}❌ Webserver is not running${NC}"
    echo "   Fix: Run 'docker-compose up -d'"
fi

if docker-compose ps | grep -q "airflow-csv-postgres-postgres-1.*Up"; then
    echo -e "${GREEN}✅ PostgreSQL is running${NC}"
else
    echo -e "${RED}❌ PostgreSQL is not running${NC}"
    echo "   Fix: Run 'docker-compose up -d postgres'"
fi

# Check 2: PostgreSQL connection
echo -e "\n${YELLOW}2. Checking PostgreSQL connection...${NC}"
if docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT 1;" &>/dev/null; then
    echo -e "${GREEN}✅ PostgreSQL connection is working${NC}"
else
    echo -e "${RED}❌ Cannot connect to PostgreSQL${NC}"
    echo "   Fix: Check if PostgreSQL container is running"
fi

# Check 3: Airflow connection
echo -e "\n${YELLOW}3. Checking Airflow PostgreSQL connection...${NC}"
conn_check=$(docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections get write_to_psql 2>&1)
if echo "$conn_check" | grep -q "postgres://"; then
    echo -e "${GREEN}✅ Airflow connection 'write_to_psql' exists${NC}"
else
    echo -e "${RED}❌ Airflow connection 'write_to_psql' not found${NC}"
    echo "   Fixing..."
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections add 'write_to_psql' \
        --conn-type 'postgres' \
        --conn-host 'postgres' \
        --conn-schema 'airflow' \
        --conn-login 'airflow' \
        --conn-password 'airflow' \
        --conn-port 5432
    echo -e "${GREEN}✅ Connection created${NC}"
fi

# Check 4: Sample files
echo -e "\n${YELLOW}4. Checking sample files...${NC}"
sample_count=$(docker exec airflow-csv-postgres-airflow-scheduler-1 ls /opt/airflow/sample_files/*.csv 2>/dev/null | wc -l)
if [ "$sample_count" -gt 0 ]; then
    echo -e "${GREEN}✅ Found $sample_count sample CSV files${NC}"
    docker exec airflow-csv-postgres-airflow-scheduler-1 ls -lh /opt/airflow/sample_files/*.csv | tail -5
else
    echo -e "${RED}❌ No sample files found${NC}"
    echo "   Generating sample files..."
    docker cp scripts/generate_sample_data.py airflow-csv-postgres-airflow-scheduler-1:/tmp/
    docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "cd /tmp && python generate_sample_data.py && cp -r sample_files /opt/airflow/"
    echo -e "${GREEN}✅ Sample files generated${NC}"
fi

# Check 5: DAG directories
echo -e "\n${YELLOW}5. Checking DAG directories...${NC}"
dirs=("sql" "sql/incremental" "incremental_data" "reports" "reports/archive")
for dir in "${dirs[@]}"; do
    if docker exec airflow-csv-postgres-airflow-scheduler-1 test -d "/opt/airflow/dags/$dir" 2>/dev/null; then
        echo -e "${GREEN}✅ Directory exists: /opt/airflow/dags/$dir${NC}"
    else
        echo -e "${RED}❌ Missing directory: /opt/airflow/dags/$dir${NC}"
        docker exec airflow-csv-postgres-airflow-scheduler-1 mkdir -p "/opt/airflow/dags/$dir"
        docker exec airflow-csv-postgres-airflow-scheduler-1 chmod 777 "/opt/airflow/dags/$dir"
        echo -e "${GREEN}   ✅ Created directory${NC}"
    fi
done

# Check 6: DAG files
echo -e "\n${YELLOW}6. Checking DAG files...${NC}"
dag_count=$(docker exec airflow-csv-postgres-airflow-scheduler-1 ls /opt/airflow/dags/*.py 2>/dev/null | wc -l)
if [ "$dag_count" -gt 0 ]; then
    echo -e "${GREEN}✅ Found $dag_count DAG files${NC}"
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list
else
    echo -e "${RED}❌ No DAG files found${NC}"
    echo "   Check if dags/ directory is properly mounted"
fi

# Check 7: Webserver accessibility
echo -e "\n${YELLOW}7. Checking Airflow webserver...${NC}"
if curl -s -o /dev/null -w "%{http_code}" http://localhost:8080 | grep -q "200\|302"; then
    echo -e "${GREEN}✅ Webserver is accessible at http://localhost:8080${NC}"
else
    echo -e "${RED}❌ Cannot access webserver${NC}"
    echo "   Check if port 8080 is available"
fi

# Check 8: Recent errors
echo -e "\n${YELLOW}8. Checking for recent errors...${NC}"
error_count=$(docker logs airflow-csv-postgres-airflow-scheduler-1 2>&1 | tail -100 | grep -i "error" | wc -l)
if [ "$error_count" -eq 0 ]; then
    echo -e "${GREEN}✅ No recent errors in scheduler logs${NC}"
else
    echo -e "${YELLOW}⚠️  Found $error_count error messages in recent logs${NC}"
    echo "   Last 5 errors:"
    docker logs airflow-csv-postgres-airflow-scheduler-1 2>&1 | tail -100 | grep -i "error" | tail -5
fi

# Fix common issues
echo -e "\n${YELLOW}9. Applying common fixes...${NC}"

# Fix permissions
echo "   • Fixing permissions..."
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "
    chmod -R 777 /opt/airflow/dags/sql 2>/dev/null
    chmod -R 777 /opt/airflow/dags/incremental_data 2>/dev/null
    chmod -R 777 /opt/airflow/dags/reports 2>/dev/null
    chown -R airflow:root /opt/airflow/dags 2>/dev/null
"
docker exec airflow-csv-postgres-airflow-webserver-1 bash -c "
    chmod -R 777 /opt/airflow/dags/sql 2>/dev/null
    chmod -R 777 /opt/airflow/dags/incremental_data 2>/dev/null
    chmod -R 777 /opt/airflow/dags/reports 2>/dev/null
    chown -R airflow:root /opt/airflow/dags 2>/dev/null
"
echo -e "${GREEN}     ✅ Permissions fixed${NC}"

# Sync sample files
echo "   • Syncing sample files..."
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "cp -r /opt/airflow/sample_files /tmp/ 2>/dev/null" || true
docker cp airflow-csv-postgres-airflow-scheduler-1:/tmp/sample_files . 2>/dev/null || true
docker cp sample_files airflow-csv-postgres-airflow-webserver-1:/opt/airflow/ 2>/dev/null || true
echo -e "${GREEN}     ✅ Sample files synced${NC}"

# Summary
echo -e "\n${YELLOW}═══════════════════════════════════════${NC}"
echo -e "${YELLOW}           TROUBLESHOOTING SUMMARY      ${NC}"
echo -e "${YELLOW}═══════════════════════════════════════${NC}"

all_good=true
if ! docker-compose ps | grep -q "Up"; then
    all_good=false
    echo -e "${RED}❌ Some containers are not running${NC}"
    echo "   Run: docker-compose up -d"
fi

if [ "$sample_count" -eq 0 ]; then
    all_good=false
    echo -e "${RED}❌ Sample files are missing${NC}"
    echo "   Run: ./fix_and_run_everything.sh"
fi

if [ "$dag_count" -eq 0 ]; then
    all_good=false
    echo -e "${RED}❌ DAG files are not visible${NC}"
    echo "   Check volume mounts in docker-compose.yml"
fi

if $all_good; then
    echo -e "${GREEN}✅ Everything looks good!${NC}"
    echo ""
    echo "Next steps:"
    echo "1. Visit http://localhost:8080 (admin/admin)"
    echo "2. Run: ./run_dags.sh to trigger DAGs"
    echo "3. Check table data: docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c '\dt'"
else
    echo ""
    echo -e "${YELLOW}Some issues were found. Try running:${NC}"
    echo "  ./fix_and_run_everything.sh"
fi

echo ""
echo -e "${YELLOW}Quick Commands:${NC}"
echo "• Restart everything: docker-compose restart"
echo "• View logs: docker-compose logs -f"
echo "• Trigger a DAG: docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger <dag_name>"
echo "• List DAGs: docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list"