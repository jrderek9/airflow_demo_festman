#!/bin/bash

# Easy DAG Runner for Airflow CSV Automation
# This script provides simple commands to run and monitor your DAGs

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
BLUE='\033[0;34m'
RED='\033[0;31m'
NC='\033[0m'

# Container name
SCHEDULER="airflow-csv-postgres-airflow-scheduler-1"
POSTGRES="airflow-csv-postgres-postgres-1"

show_menu() {
    echo -e "${BLUE}╔════════════════════════════════════════════╗${NC}"
    echo -e "${BLUE}║       AIRFLOW DAG CONTROL CENTER          ║${NC}"
    echo -e "${BLUE}╚════════════════════════════════════════════╝${NC}"
    echo ""
    echo "Choose an option:"
    echo ""
    echo "  1) Run Simple CSV Test (5 rows)"
    echo "  2) Process Large CSV Files (875,000 rows)"
    echo "  3) Start Hourly Incremental Processing"
    echo "  4) Run Data Monitoring & Reports"
    echo "  5) Check All Table Row Counts"
    echo "  6) View Latest Executive Report"
    echo "  7) List All DAGs Status"
    echo "  8) View Recent DAG Runs"
    echo "  9) Trigger ALL DAGs at Once"
    echo "  0) Exit"
    echo ""
}

run_simple_csv() {
    echo -e "${YELLOW}Running Simple CSV Test DAG...${NC}"
    docker exec $SCHEDULER airflow dags trigger simple_csv_to_postgres_dag
    echo -e "${GREEN}✅ Triggered! Check progress at http://localhost:8080${NC}"
    sleep 5
    echo -e "\n${YELLOW}Checking results...${NC}"
    docker exec $POSTGRES psql -U airflow -d airflow -c "SELECT * FROM sample_table;" 2>/dev/null || echo "Table not created yet. Check Airflow UI for progress."
}

run_large_csv() {
    echo -e "${YELLOW}Processing Large CSV Files...${NC}"
    echo "This will process:"
    echo "  • 300,000 employee records"
    echo "  • 100,000 product records"
    echo "  • 150,000 sales transactions"
    echo ""
    read -p "Continue? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker exec $SCHEDULER airflow dags trigger large_csv_processor_dag
        echo -e "${GREEN}✅ Started processing! This may take several minutes.${NC}"
        echo -e "${GREEN}Monitor progress at http://localhost:8080${NC}"
    fi
}

start_incremental() {
    echo -e "${YELLOW}Starting Hourly Incremental Processing...${NC}"
    docker exec $SCHEDULER airflow dags unpause incremental_data_processor
    echo -e "${GREEN}✅ Incremental processor is now active!${NC}"
    echo "It will run every hour and add:"
    echo "  • 1,000 new employees"
    echo "  • 500 new products"
    echo "  • 2,000 new sales"
    echo "  • 1,500 new support tickets"
    echo "  • 3,000 new web analytics records"
    echo "  • 800 new healthcare records"
    echo ""
    read -p "Trigger first run now? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        docker exec $SCHEDULER airflow dags trigger incremental_data_processor
        echo -e "${GREEN}✅ First incremental run triggered!${NC}"
    fi
}

run_monitoring() {
    echo -e "${YELLOW}Running Data Monitoring & Reporting...${NC}"
    docker exec $SCHEDULER airflow dags unpause data_monitoring_and_reporting
    docker exec $SCHEDULER airflow dags trigger data_monitoring_and_reporting
    echo -e "${GREEN}✅ Monitoring DAG triggered!${NC}"
    echo "This will generate:"
    echo "  • Data quality reports"
    echo "  • Executive summary"
    echo "  • Performance metrics"
    echo "  • Database views for dashboards"
}

check_table_counts() {
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    echo -e "${BLUE}     DATABASE TABLE ROW COUNTS         ${NC}"
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    
    tables=("sample_table" "employees" "products" "sales_transactions" "support_tickets" "web_analytics" "healthcare_records")
    
    total_rows=0
    for table in "${tables[@]}"; do
        count=$(docker exec $POSTGRES psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM $table;" 2>/dev/null | tr -d ' ' || echo "0")
        if [ "$count" != "0" ] && [ -n "$count" ]; then
            printf "%-25s %10s rows\n" "$table:" "$count"
            total_rows=$((total_rows + count))
        else
            printf "%-25s %10s\n" "$table:" "Not created"
        fi
    done
    echo -e "${BLUE}═══════════════════════════════════════${NC}"
    printf "%-25s %10s rows\n" "TOTAL:" "$total_rows"
}

view_executive_report() {
    echo -e "${YELLOW}Fetching latest executive report...${NC}"
    docker exec $SCHEDULER bash -c "ls -t /opt/airflow/dags/reports/executive_summary_*.json 2>/dev/null | head -1 | xargs cat" 2>/dev/null || echo "No reports generated yet. Run the monitoring DAG first."
}

list_dag_status() {
    echo -e "${BLUE}DAG Status Overview${NC}"
    echo "==================="
    docker exec $SCHEDULER airflow dags list-runs -d simple_csv_to_postgres_dag --limit 3 2>/dev/null
    echo ""
    docker exec $SCHEDULER airflow dags list-runs -d incremental_data_processor --limit 3 2>/dev/null
    echo ""
    docker exec $SCHEDULER airflow dags list-runs -d data_monitoring_and_reporting --limit 3 2>/dev/null
}

view_recent_runs() {
    echo -e "${BLUE}Recent DAG Runs (Last 24 Hours)${NC}"
    echo "================================"
    docker exec $SCHEDULER airflow dags list-runs --limit 10
}

trigger_all_dags() {
    echo -e "${RED}⚠️  WARNING: This will trigger ALL DAGs!${NC}"
    echo "This includes:"
    echo "  • Simple CSV test"
    echo "  • Large CSV processor (875,000 rows)"
    echo "  • Incremental processor"
    echo "  • Monitoring & reporting"
    echo ""
    read -p "Are you sure? (y/n) " -n 1 -r
    echo
    if [[ $REPLY =~ ^[Yy]$ ]]; then
        echo -e "${YELLOW}Triggering all DAGs...${NC}"
        docker exec $SCHEDULER airflow dags trigger simple_csv_to_postgres_dag
        docker exec $SCHEDULER airflow dags trigger large_csv_processor_dag
        docker exec $SCHEDULER airflow dags unpause incremental_data_processor
        docker exec $SCHEDULER airflow dags trigger incremental_data_processor
        docker exec $SCHEDULER airflow dags unpause data_monitoring_and_reporting
        docker exec $SCHEDULER airflow dags trigger data_monitoring_and_reporting
        echo -e "${GREEN}✅ All DAGs triggered!${NC}"
        echo -e "${GREEN}Monitor progress at http://localhost:8080${NC}"
    fi
}

# Main loop
while true; do
    clear
    show_menu
    read -p "Enter your choice (0-9): " choice
    
    case $choice in
        1) run_simple_csv ;;
        2) run_large_csv ;;
        3) start_incremental ;;
        4) run_monitoring ;;
        5) check_table_counts ;;
        6) view_executive_report ;;
        7) list_dag_status ;;
        8) view_recent_runs ;;
        9) trigger_all_dags ;;
        0) echo "Goodbye!"; exit 0 ;;
        *) echo -e "${RED}Invalid option!${NC}" ;;
    esac
    
    echo ""
    read -p "Press Enter to continue..."
done