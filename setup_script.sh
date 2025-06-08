#!/bin/bash

# Complete setup and run script for Airflow CSV Automation
# This script will setup everything and run all DAGs

set -e  # Exit on error

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
NC='\033[0m' # No Color

# Container names based on your docker-compose output
SCHEDULER_CONTAINER="airflow-csv-postgres-airflow-scheduler-1"
WEBSERVER_CONTAINER="airflow-csv-postgres-airflow-webserver-1"
POSTGRES_CONTAINER="airflow-csv-postgres-postgres-1"

echo -e "${BLUE}================================================${NC}"
echo -e "${BLUE}Airflow CSV Automation - Complete Setup & Run${NC}"
echo -e "${BLUE}================================================${NC}"

# Function to check if containers are running
check_containers() {
    echo -e "\n${YELLOW}Checking containers...${NC}"
    
    if ! docker ps | grep -q "$SCHEDULER_CONTAINER"; then
        echo -e "${RED}Error: Scheduler container not running!${NC}"
        echo "Starting containers..."
        docker-compose up -d
        sleep 10
    fi
    
    echo -e "${GREEN}✓ All containers are running${NC}"
}

# Function to wait for Airflow to be ready
wait_for_airflow() {
    echo -e "\n${YELLOW}Waiting for Airflow to be ready...${NC}"
    
    # Wait for webserver to be healthy
    local max_attempts=30
    local attempt=1
    
    while [ $attempt -le $max_attempts ]; do
        if curl -s http://localhost:8080/health >/dev/null 2>&1; then
            echo -e "${GREEN}✓ Airflow is ready!${NC}"
            return 0
        fi
        echo -n "."
        sleep 2
        ((attempt++))
    done
    
    echo -e "\n${RED}Airflow did not become ready in time${NC}"
    return 1
}

# Step 1: Create necessary directories
setup_directories() {
    echo -e "\n${YELLOW}Step 1: Setting up directories...${NC}"
    
    # Create directories locally
    mkdir -p dags/sql/incremental
    mkdir -p dags/incremental_data
    mkdir -p dags/reports/archive
    mkdir -p logs
    mkdir -p plugins
    mkdir -p sample_files
    mkdir -p backups
    
    # Set permissions
    chmod -R 777 dags/sql
    chmod -R 777 dags/incremental_data
    chmod -R 777 dags/reports
    chmod -R 777 logs
    
    # Create directories in containers
    docker exec "$SCHEDULER_CONTAINER" bash -c "
        mkdir -p /opt/airflow/dags/sql/incremental
        mkdir -p /opt/airflow/dags/incremental_data
        mkdir -p /opt/airflow/dags/reports/archive
        chmod -R 777 /opt/airflow/dags/sql
        chmod -R 777 /opt/airflow/dags/incremental_data
        chmod -R 777 /opt/airflow/dags/reports
    " || true
    
    docker exec "$WEBSERVER_CONTAINER" bash -c "
        mkdir -p /opt/airflow/dags/sql/incremental
        mkdir -p /opt/airflow/dags/incremental_data
        mkdir -p /opt/airflow/dags/reports/archive
        chmod -R 777 /opt/airflow/dags/sql
        chmod -R 777 /opt/airflow/dags/incremental_data
        chmod -R 777 /opt/airflow/dags/reports
    " || true
    
    echo -e "${GREEN}✓ Directories created${NC}"
}

# Step 2: Generate sample data
generate_sample_data() {
    echo -e "\n${YELLOW}Step 2: Generating sample data...${NC}"
    
    # Check if data already exists
    if [ -f "sample_files/employees_large.csv" ]; then
        echo -e "${GREEN}✓ Sample data already exists${NC}"
        return 0
    fi
    
    # Copy the generate script to container and run it
    docker cp scripts/generate_sample_data.py "$SCHEDULER_CONTAINER":/tmp/
    
    docker exec "$SCHEDULER_CONTAINER" bash -c "
        cd /opt/airflow
        python /tmp/generate_sample_data.py
    "
    
    # Copy generated files back to host
    docker cp "$SCHEDULER_CONTAINER":/opt/airflow/sample_files ./
    
    # Also copy to webserver
    docker cp sample_files "$WEBSERVER_CONTAINER":/opt/airflow/
    
    echo -e "${GREEN}✓ Sample data generated${NC}"
}

# Step 3: Configure PostgreSQL connection
configure_postgres_connection() {
    echo -e "\n${YELLOW}Step 3: Configuring PostgreSQL connection...${NC}"
    
    # Check if connection already exists
    docker exec "$SCHEDULER_CONTAINER" airflow connections get write_to_psql >/dev/null 2>&1
    if [ $? -eq 0 ]; then
        echo -e "${GREEN}✓ PostgreSQL connection already configured${NC}"
        return 0
    fi
    
    # Add the connection
    docker exec "$SCHEDULER_CONTAINER" airflow connections add 'write_to_psql' \
        --conn-type 'postgres' \
        --conn-host 'postgres' \
        --conn-schema 'airflow' \
        --conn-login 'airflow' \
        --conn-password 'airflow' \
        --conn-port 5432
    
    echo -e "${GREEN}✓ PostgreSQL connection configured${NC}"
}

# Step 4: Test DAGs
test_dags() {
    echo -e "\n${YELLOW}Step 4: Testing DAGs...${NC}"
    
    # List all DAGs
    echo -e "\n${BLUE}Available DAGs:${NC}"
    docker exec "$SCHEDULER_CONTAINER" airflow dags list
    
    # Test simple_test_dag
    echo -e "\n${BLUE}Testing simple_test_dag...${NC}"
    docker exec "$SCHEDULER_CONTAINER" airflow dags test simple_test_dag 2024-01-01 || true
}

# Step 5: Run DAGs
run_dags() {
    echo -e "\n${YELLOW}Step 5: Running DAGs...${NC}"
    
    # 1. Run simple_csv_to_postgres_dag
    echo -e "\n${BLUE}Running simple_csv_to_postgres_dag...${NC}"
    docker exec "$SCHEDULER_CONTAINER" airflow dags trigger simple_csv_to_postgres_dag
    
    # 2. Unpause and run incremental_data_processor
    echo -e "\n${BLUE}Unpausing incremental_data_processor...${NC}"
    docker exec "$SCHEDULER_CONTAINER" airflow dags unpause incremental_data_processor
    
    # 3. Unpause and run data_monitoring_and_reporting
    echo -e "\n${BLUE}Unpausing data_monitoring_and_reporting...${NC}"
    docker exec "$SCHEDULER_CONTAINER" airflow dags unpause data_monitoring_and_reporting
    
    # 4. Run large_csv_processor_dag
    echo -e "\n${BLUE}Running large_csv_processor_dag...${NC}"
    docker exec "$SCHEDULER_CONTAINER" airflow dags trigger large_csv_processor_dag
    
    echo -e "${GREEN}✓ All DAGs triggered${NC}"
}

# Step 6: Monitor DAG runs
monitor_dag_runs() {
    echo -e "\n${YELLOW}Step 6: Monitoring DAG runs...${NC}"
    
    # Show running DAGs
    echo -e "\n${BLUE}Current DAG runs:${NC}"
    docker exec "$SCHEDULER_CONTAINER" airflow dags list-runs \
        --dag-id simple_csv_to_postgres_dag \
        --limit 5
    
    # Check database tables
    echo -e "\n${BLUE}Checking database tables:${NC}"
    docker exec "$POSTGRES_CONTAINER" psql -U airflow -d airflow -c "\dt"
    
    # Check sample_table data
    echo -e "\n${BLUE}Sample table data:${NC}"
    docker exec "$POSTGRES_CONTAINER" psql -U airflow -d airflow -c "SELECT COUNT(*) FROM sample_table;" 2>/dev/null || echo "Table not yet created"
}

# Step 7: View reports (if any exist)
view_reports() {
    echo -e "\n${YELLOW}Step 7: Checking for reports...${NC}"
    
    # List reports
    docker exec "$SCHEDULER_CONTAINER" bash -c "ls -la /opt/airflow/dags/reports/*.json 2>/dev/null" || echo "No reports generated yet"
}

# Main execution
main() {
    echo -e "${GREEN}Starting complete setup and run process...${NC}"
    
    # Check if containers are running
    check_containers
    
    # Wait for Airflow to be ready
    wait_for_airflow
    
    # Run all setup steps
    setup_directories
    generate_sample_data
    configure_postgres_connection
    test_dags
    run_dags
    
    # Wait a bit for DAGs to start processing
    echo -e "\n${YELLOW}Waiting for DAGs to process...${NC}"
    sleep 30
    
    # Monitor results
    monitor_dag_runs
    view_reports
    
    echo -e "\n${GREEN}================================================${NC}"
    echo -e "${GREEN}✅ SETUP COMPLETE!${NC}"
    echo -e "${GREEN}================================================${NC}"
    echo -e "\n${BLUE}Access Points:${NC}"
    echo -e "  • Airflow UI: ${GREEN}http://localhost:8080${NC} (admin/admin)"
    echo -e "  • pgAdmin: ${GREEN}http://localhost:5050${NC} (admin@pgadmin.com/admin)"
    echo -e "\n${BLUE}Useful Commands:${NC}"
    echo -e "  • View logs: ${YELLOW}docker logs $SCHEDULER_CONTAINER${NC}"
    echo -e "  • List DAG runs: ${YELLOW}docker exec $SCHEDULER_CONTAINER airflow dags list-runs --dag-id <dag_id>${NC}"
    echo -e "  • Trigger DAG: ${YELLOW}docker exec $SCHEDULER_CONTAINER airflow dags trigger <dag_id>${NC}"
    echo -e "  • View database: ${YELLOW}docker exec $POSTGRES_CONTAINER psql -U airflow -d airflow${NC}"
}

# Parse command line arguments
case "$1" in
    "quick")
        # Quick run - just trigger DAGs
        check_containers
        run_dags
        ;;
    "monitor")
        # Just monitor
        monitor_dag_runs
        view_reports
        ;;
    "clean")
        # Clean and restart
        echo "Cleaning up..."
        docker-compose down -v
        rm -rf logs/* dags/sql/* dags/incremental_data/* dags/reports/*
        docker-compose up -d
        ;;
    *)
        # Full setup and run
        main
        ;;
esac