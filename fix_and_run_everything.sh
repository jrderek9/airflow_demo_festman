#!/bin/bash

# Complete Fix and Setup Script for Airflow CSV Automation
# This script will fix ALL issues and get your DAGs running

echo "🚀 COMPLETE AIRFLOW SETUP AND FIX SCRIPT"
echo "========================================"
echo "This will fix everything and get your DAGs running!"
echo ""

# Set colors for output
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m' # No Color

# Step 1: Stop any running containers and clean up
echo -e "${YELLOW}Step 1: Cleaning up existing setup...${NC}"
docker-compose down
docker volume prune -f

# Step 2: Fix the .env file
echo -e "${YELLOW}Step 2: Setting up environment...${NC}"
echo "AIRFLOW_UID=$(id -u)" > .env
echo "COMPOSE_PROJECT_NAME=airflow-csv-postgres" >> .env
echo "_PIP_ADDITIONAL_REQUIREMENTS=" >> .env

# Step 3: Create all necessary directories
echo -e "${YELLOW}Step 3: Creating directories...${NC}"
mkdir -p dags/sql/incremental
mkdir -p dags/incremental_data
mkdir -p dags/reports/archive
mkdir -p logs
mkdir -p plugins
mkdir -p sample_files
mkdir -p backups

# Set proper permissions
chmod -R 755 dags/
chmod -R 755 logs/
chmod -R 755 sample_files/

# Step 4: Start PostgreSQL first
echo -e "${YELLOW}Step 4: Starting PostgreSQL...${NC}"
docker-compose up -d postgres
sleep 10

# Step 5: Initialize Airflow
echo -e "${YELLOW}Step 5: Initializing Airflow...${NC}"
docker-compose run --rm airflow-init

# Step 6: Start all services
echo -e "${YELLOW}Step 6: Starting all Airflow services...${NC}"
docker-compose up -d
sleep 20

# Step 7: Generate sample data
echo -e "${YELLOW}Step 7: Generating sample data files...${NC}"
# First, copy the generate script to the scheduler container
docker cp scripts/generate_sample_data.py airflow-csv-postgres-airflow-scheduler-1:/tmp/

# Execute the script inside the container
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "
    cd /tmp
    python generate_sample_data.py
    cp -r sample_files /opt/airflow/
"

# Copy to webserver too
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "cp -r /opt/airflow/sample_files /tmp/"
docker cp airflow-csv-postgres-airflow-scheduler-1:/tmp/sample_files .
docker cp sample_files airflow-csv-postgres-airflow-webserver-1:/opt/airflow/

# Step 8: Set up PostgreSQL connection
echo -e "${YELLOW}Step 8: Configuring PostgreSQL connection...${NC}"
sleep 5
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow connections add 'write_to_psql' \
    --conn-type 'postgres' \
    --conn-host 'postgres' \
    --conn-schema 'airflow' \
    --conn-login 'airflow' \
    --conn-password 'airflow' \
    --conn-port 5432 || true

# Step 9: Fix directory permissions in containers
echo -e "${YELLOW}Step 9: Fixing directory permissions...${NC}"
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c "
    mkdir -p /opt/airflow/dags/sql/incremental
    mkdir -p /opt/airflow/dags/incremental_data
    mkdir -p /opt/airflow/dags/reports/archive
    chmod -R 777 /opt/airflow/dags/sql
    chmod -R 777 /opt/airflow/dags/incremental_data
    chmod -R 777 /opt/airflow/dags/reports
    chown -R airflow:root /opt/airflow/dags
"

docker exec airflow-csv-postgres-airflow-webserver-1 bash -c "
    mkdir -p /opt/airflow/dags/sql/incremental
    mkdir -p /opt/airflow/dags/incremental_data
    mkdir -p /opt/airflow/dags/reports/archive
    chmod -R 777 /opt/airflow/dags/sql
    chmod -R 777 /opt/airflow/dags/incremental_data
    chmod -R 777 /opt/airflow/dags/reports
    chown -R airflow:root /opt/airflow/dags
"

# Step 10: Test the simple DAG
echo -e "${YELLOW}Step 10: Testing simple DAG...${NC}"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags test simple_csv_to_postgres_dag 2024-01-01

# Step 11: List all DAGs
echo -e "${YELLOW}Step 11: Listing all available DAGs...${NC}"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list

# Step 12: Trigger DAGs
echo -e "${YELLOW}Step 12: Triggering DAGs...${NC}"
# Trigger simple CSV DAG
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_csv_to_postgres_dag
echo -e "${GREEN}✅ Triggered simple_csv_to_postgres_dag${NC}"

# Unpause incremental processor
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause incremental_data_processor
echo -e "${GREEN}✅ Unpaused incremental_data_processor (will run hourly)${NC}"

# Unpause monitoring DAG
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags unpause data_monitoring_and_reporting
echo -e "${GREEN}✅ Unpaused data_monitoring_and_reporting (will run every 6 hours)${NC}"

# Step 13: Create helper script for common commands
echo -e "${YELLOW}Step 13: Creating helper commands script...${NC}"
cat > airflow_commands.sh << 'EOF'
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
EOF

chmod +x airflow_commands.sh

# Step 14: Final status check
echo ""
echo -e "${GREEN}✅ SETUP COMPLETE!${NC}"
echo "=================="
echo ""
echo "📊 Status:"
docker-compose ps
echo ""
echo "🌐 Access Points:"
echo "  - Airflow UI: http://localhost:8080 (admin/admin)"
echo "  - pgAdmin: http://localhost:5050 (admin@pgadmin.com/admin)"
echo ""
echo "📁 Generated Files:"
ls -la sample_files/*.csv 2>/dev/null | tail -5
echo ""
echo "🎯 Quick Commands:"
echo "  - List DAGs: docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list"
echo "  - Trigger simple DAG: docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger simple_csv_to_postgres_dag"
echo "  - Check data: ./airflow_commands.sh check-data"
echo "  - Trigger all DAGs: ./airflow_commands.sh trigger-all"
echo ""
echo "📝 Next Steps:"
echo "  1. Open http://localhost:8080 in your browser"
echo "  2. Login with admin/admin"
echo "  3. Check the DAGs page to see all running DAGs"
echo "  4. Monitor the progress in the UI"
echo ""
echo -e "${YELLOW}Note: The incremental processor runs hourly and monitoring runs every 6 hours.${NC}"
echo -e "${YELLOW}You can manually trigger them anytime from the UI or command line.${NC}"

# Create a quick test script
cat > quick_test.sh << 'EOF'
#!/bin/bash
echo "🧪 Running quick test..."
echo "1. Checking if services are running:"
docker-compose ps

echo -e "\n2. Checking PostgreSQL connection:"
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT version();"

echo -e "\n3. Checking sample files:"
docker exec airflow-csv-postgres-airflow-scheduler-1 ls -la /opt/airflow/sample_files/

echo -e "\n4. Checking DAGs:"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list

echo -e "\n5. Checking tables in database:"
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "\dt"

echo -e "\n✅ Quick test complete!"
EOF

chmod +x quick_test.sh

echo ""
echo -e "${GREEN}💡 TIP: Run ./quick_test.sh to verify everything is working!${NC}"