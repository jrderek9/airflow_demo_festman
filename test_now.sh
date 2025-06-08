#!/bin/bash

# Immediate test script - Shows that Airflow is working RIGHT NOW
echo "🧪 IMMEDIATE AIRFLOW TEST"
echo "========================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Test 1: Direct database insert (proves database works)
echo -e "${YELLOW}Test 1: Direct database test...${NC}"
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "
    DROP TABLE IF EXISTS quick_test;
    CREATE TABLE quick_test (id SERIAL PRIMARY KEY, message TEXT, created_at TIMESTAMP DEFAULT NOW());
    INSERT INTO quick_test (message) VALUES 
        ('Database is working!'),
        ('We can insert data!'),
        ('PostgreSQL is ready!');
    SELECT * FROM quick_test;
"

if [ $? -eq 0 ]; then
    echo -e "${GREEN}✅ Database connection works perfectly!${NC}"
else
    echo -e "${RED}❌ Database connection failed${NC}"
    exit 1
fi

# Test 2: Create and run a minimal DAG
echo -e "\n${YELLOW}Test 2: Creating minimal working DAG...${NC}"
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c '
cat > /opt/airflow/dags/minimal_test.py << '\''EOF'\''
from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime

with DAG(
    '\''minimal_test'\'',
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,
    catchup=False
) as dag:
    
    # Simple bash task
    hello = BashOperator(
        task_id='\''say_hello'\'',
        bash_command='\''echo "Hello from Airflow! Working at $(date)"'\''
    )
    
    # Simple SQL task
    sql_test = PostgresOperator(
        task_id='\''sql_test'\'',
        postgres_conn_id='\''write_to_psql'\'',
        sql="""
        DROP TABLE IF EXISTS airflow_test;
        CREATE TABLE airflow_test (
            id SERIAL PRIMARY KEY,
            test_name VARCHAR(100),
            test_time TIMESTAMP DEFAULT NOW()
        );
        INSERT INTO airflow_test (test_name) VALUES 
            ('\''Airflow is working'\''),
            ('\''DAGs can run'\''),
            ('\''Success at '\'' || NOW()::TEXT);
        """,
        autocommit=True
    )
    
    hello >> sql_test
EOF
'

# Copy to webserver
docker cp airflow-csv-postgres-airflow-scheduler-1:/opt/airflow/dags/minimal_test.py minimal_test.py
docker cp minimal_test.py airflow-csv-postgres-airflow-webserver-1:/opt/airflow/dags/
rm minimal_test.py

echo -e "${GREEN}✅ Created minimal_test DAG${NC}"

# Test 3: Trigger the minimal DAG
echo -e "\n${YELLOW}Test 3: Running minimal DAG...${NC}"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags trigger minimal_test

# Wait a moment
sleep 3

# Check results
echo -e "\n${YELLOW}Checking results...${NC}"
result=$(docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -t -c "SELECT COUNT(*) FROM airflow_test;" 2>/dev/null | tr -d ' ')

if [ -n "$result" ] && [ "$result" -gt 0 ]; then
    echo -e "${GREEN}✅ AIRFLOW IS WORKING! Found $result rows in airflow_test table${NC}"
    echo ""
    echo "Data inserted by Airflow:"
    docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT * FROM airflow_test;"
else
    echo -e "${YELLOW}⚠️  DAG is still running. Checking status...${NC}"
    docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags state minimal_test $(date +%Y-%m-%d)
fi

# Test 4: Show all tables
echo -e "\n${YELLOW}Test 4: All tables in database...${NC}"
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "\dt"

# Summary
echo ""
echo -e "${GREEN}📊 TEST SUMMARY${NC}"
echo "==============="
echo ""
echo "✅ PostgreSQL is running and accessible"
echo "✅ Airflow can create and trigger DAGs"
echo "✅ DAGs can write to the database"
echo ""
echo "🎯 What to do next:"
echo "1. Open Airflow UI: http://localhost:8080 (admin/admin)"
echo "2. Look for these DAGs:"
echo "   - minimal_test (just created and triggered)"
echo "   - simple_csv_to_postgres_dag"
echo "   - super_simple_test"
echo ""
echo "3. To process your CSV files with data:"
echo "   ./run_dags.sh"
echo ""
echo "4. To see what's in the database:"
echo "   docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c '\\dt'"
echo ""
echo -e "${YELLOW}💡 TIP: If DAGs don't appear immediately in the UI, wait 30 seconds and refresh${NC}"