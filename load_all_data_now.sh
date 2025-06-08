#!/bin/bash

# Load All Data Now - Bypasses DAG issues and loads all 875,000 rows directly
echo "🚀 LOADING ALL 875,000 ROWS NOW"
echo "==============================="
echo "This will load all your CSV data directly into PostgreSQL"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

# Confirm
read -p "This will load 875,000 rows. Continue? (y/n) " -n 1 -r
echo
if [[ ! $REPLY =~ ^[Yy]$ ]]; then
    echo "Cancelled."
    exit 1
fi

echo -e "\n${YELLOW}Loading data from CSV files...${NC}"

# Create Python script to load all data
docker exec airflow-csv-postgres-airflow-scheduler-1 python3 << 'EOF'
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os
import time

# Database connection
conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow"
)
cursor = conn.cursor()

# CSV files to load
csv_files = {
    'employees_large.csv': ('employees', 300000),
    'product_inventory.csv': ('products', 100000),
    'sales_transactions.csv': ('sales_transactions', 150000),
    'customer_support_tickets.csv': ('support_tickets', 75000),
    'website_analytics.csv': ('web_analytics', 200000),
    'healthcare_records.csv': ('healthcare_records', 50000)
}

total_start = time.time()
grand_total = 0

for csv_file, (table_name, expected_rows) in csv_files.items():
    csv_path = f"/opt/airflow/sample_files/{csv_file}"
    
    if not os.path.exists(csv_path):
        print(f"❌ {csv_file} not found")
        continue
    
    print(f"\n📊 Loading {csv_file} into {table_name} table...")
    start_time = time.time()
    
    try:
        # Read CSV in chunks
        chunk_size = 10000
        total_rows = 0
        
        for chunk_num, chunk in enumerate(pd.read_csv(csv_path, chunksize=chunk_size)):
            # Clean the data
            chunk = chunk.where(pd.notnull(chunk), None)
            
            # Prepare data for insertion
            columns = chunk.columns.tolist()
            values = [tuple(row) for row in chunk.values]
            
            # Build insert query
            insert_query = f"""
                INSERT INTO {table_name} ({', '.join(columns)})
                VALUES %s
                ON CONFLICT DO NOTHING
            """
            
            # Execute batch insert
            execute_values(cursor, insert_query, values)
            total_rows += len(chunk)
            
            # Progress update
            if (chunk_num + 1) % 5 == 0:
                print(f"  Processed {total_rows:,} rows...")
        
        conn.commit()
        elapsed = time.time() - start_time
        print(f"✅ Loaded {total_rows:,} rows in {elapsed:.1f} seconds")
        grand_total += total_rows
        
    except Exception as e:
        print(f"❌ Error loading {csv_file}: {str(e)[:100]}")
        conn.rollback()
        
        # Try to create the table if it doesn't exist
        if "does not exist" in str(e):
            print(f"  Table {table_name} doesn't exist. Please run the table creation DAG first.")

# Show summary
print(f"\n{'='*50}")
print(f"✅ LOADING COMPLETE!")
print(f"{'='*50}")
print(f"Total rows loaded: {grand_total:,}")
print(f"Total time: {time.time() - total_start:.1f} seconds")

# Verify data
print("\n📊 Verification:")
tables = ['employees', 'products', 'sales_transactions', 'support_tickets', 'web_analytics', 'healthcare_records']
for table in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table}: {count:,} rows")
    except:
        print(f"  {table}: table not found")

cursor.close()
conn.close()
EOF

echo ""
echo -e "${GREEN}🎉 DATA LOADING COMPLETE!${NC}"
echo "========================="
echo ""
echo "✅ Your data has been loaded into PostgreSQL"
echo ""
echo "📊 To verify the data:"
echo "  docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c \"SELECT 'employees', COUNT(*) FROM employees UNION ALL SELECT 'products', COUNT(*) FROM products UNION ALL SELECT 'sales', COUNT(*) FROM sales_transactions;\""
echo ""
echo "🔍 To explore the data:"
echo "  docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow"
echo "  Then run: SELECT * FROM employees LIMIT 10;"
echo ""
echo "📈 To see table sizes:"
echo "  ./check_status.sh"