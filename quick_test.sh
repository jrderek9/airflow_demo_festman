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
