#!/bin/bash

echo "🔧 Fixing All DAGs and Setup Issues"
echo "==================================="
echo ""

# 1. Create required directories in both containers
echo "1. Creating required directories..."
for container in airflow-scheduler-1 airflow-webserver-1; do
    echo "   Setting up directories in $container..."
    docker exec airflow-csv-postgres-$container bash -c "
        mkdir -p /opt/airflow/dags/sql/incremental
        mkdir -p /opt/airflow/dags/incremental_data
        mkdir -p /opt/airflow/dags/reports/archive
        chmod -R 755 /opt/airflow/dags/
        echo '   ✅ Directories created'
    "
done

# 2. Generate sample data
echo ""
echo "2. Generating sample CSV files..."
# First, check if sample_files exist
if [ ! -d "sample_files" ] || [ -z "$(ls -A sample_files 2>/dev/null)" ]; then
    echo "   Sample files missing. Generating..."
    
    # Run the data generation script
    if [ -f "scripts/generate_sample_data.py" ]; then
        python3 scripts/generate_sample_data.py
    else
        echo "   ⚠️  generate_sample_data.py not found in scripts/"
    fi
    
    # Copy to containers
    if [ -d "sample_files" ]; then
        docker cp sample_files airflow-csv-postgres-airflow-scheduler-1:/opt/airflow/
        docker cp sample_files airflow-csv-postgres-airflow-webserver-1:/opt/airflow/
        echo "   ✅ Sample files copied to containers"
    fi
else
    echo "   ✅ Sample files already exist"
fi

# 3. Fix the DAG files by removing the if __name__ == "__main__" blocks
echo ""
echo "3. Fixing DAG syntax issues..."
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c '
cd /opt/airflow/dags

# Remove the problematic if __name__ == "__main__" blocks from DAGs
for dag_file in incremental_data_processor.py data_monitoring_and_reporting.py; do
    if [ -f "$dag_file" ]; then
        echo "   Fixing $dag_file..."
        # Remove the last 2 lines if they contain the main block
        sed -i "/if __name__ == \"__main__\":/,/dag.cli()/d" "$dag_file"
        echo "   ✅ Fixed $dag_file"
    fi
done
'

# 4. Test all DAGs again
echo ""
echo "4. Testing all DAGs..."
docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c '
cd /opt/airflow/dags
echo "Available DAG files:"
ls -la *.py
echo ""

# Test each DAG
for dag_file in *.py; do
    echo "Testing: $dag_file"
    python3 -c "import sys; sys.path.insert(0, \"/opt/airflow/dags\"); exec(open(\"$dag_file\").read())" 2>&1 | grep -E "(Error|Exception|ImportError)" | head -5
    if [ ${PIPESTATUS[0]} -eq 0 ]; then
        echo "✅ $dag_file - OK"
    else
        echo "⚠️  $dag_file - Has issues"
    fi
    echo ""
done
'

# 5. Refresh DAGs in Airflow
echo ""
echo "5. Refreshing DAGs in Airflow..."
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags reserialize

# Wait a moment for DAGs to be processed
sleep 5

# 6. List all recognized DAGs
echo ""
echo "6. Final DAG list:"
docker exec airflow-csv-postgres-airflow-scheduler-1 airflow dags list

echo ""
echo "✅ Setup complete!"
echo ""
echo "📊 Next steps:"
echo "1. Access Airflow UI: http://localhost:8080 (admin/admin)"
echo "2. Check if all 5 DAGs are visible"
echo "3. If DAGs are missing, check the Airflow UI 'Import Errors' section"