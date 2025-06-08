#!/bin/bash

echo "🚀 Quick Data Load to PostgreSQL"
echo "================================"
echo ""

# Load sample data directly into PostgreSQL
docker exec airflow-csv-postgres-airflow-scheduler-1 python3 << 'PYEOF'
import psycopg2
import random
from datetime import datetime, timedelta

print("Connecting to PostgreSQL...")
try:
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL")
    
    # Create a simple test table with data
    print("\nCreating and loading test tables...")
    
    # 1. Simple test table
    cursor.execute("""
        DROP TABLE IF EXISTS test_data CASCADE;
        CREATE TABLE test_data (
            id SERIAL PRIMARY KEY,
            name VARCHAR(100),
            value DECIMAL(10,2),
            category VARCHAR(50),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    
    # Insert test data
    categories = ['Electronics', 'Clothing', 'Food', 'Books', 'Sports']
    for i in range(1, 101):
        cursor.execute("""
            INSERT INTO test_data (name, value, category)
            VALUES (%s, %s, %s)
        """, (
            f'Item {i}',
            round(random.uniform(10, 1000), 2),
            random.choice(categories)
        ))
    
    # 2. Create employees table with sample data
    cursor.execute("""
        DROP TABLE IF EXISTS employees CASCADE;
        CREATE TABLE employees (
            employee_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            department VARCHAR(50),
            salary DECIMAL(10,2),
            hire_date DATE,
            is_active BOOLEAN DEFAULT TRUE
        );
    """)
    
    departments = ['IT', 'HR', 'Sales', 'Marketing', 'Finance']
    first_names = ['John', 'Jane', 'Mike', 'Sarah', 'Tom', 'Emily']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Davis']
    
    for i in range(1, 51):
        cursor.execute("""
            INSERT INTO employees (first_name, last_name, department, salary, hire_date)
            VALUES (%s, %s, %s, %s, %s)
        """, (
            random.choice(first_names),
            random.choice(last_names),
            random.choice(departments),
            round(random.uniform(40000, 120000), 2),
            (datetime.now() - timedelta(days=random.randint(30, 1000))).date()
        ))
    
    # 3. Create products table
    cursor.execute("""
        DROP TABLE IF EXISTS products CASCADE;
        CREATE TABLE products (
            product_id VARCHAR(20) PRIMARY KEY,
            product_name VARCHAR(200),
            category VARCHAR(50),
            unit_price DECIMAL(10,2),
            quantity_in_stock INTEGER,
            is_active BOOLEAN DEFAULT TRUE
        );
    """)
    
    for i in range(1, 31):
        cursor.execute("""
            INSERT INTO products VALUES (%s, %s, %s, %s, %s, %s)
        """, (
            f'PROD{i:04d}',
            f'Product {i}',
            random.choice(categories),
            round(random.uniform(10, 500), 2),
            random.randint(0, 1000),
            True
        ))
    
    conn.commit()
    print("✅ Tables created and data loaded!")
    
    # Show summary
    tables = ['test_data', 'employees', 'products']
    print("\n📊 Data Summary:")
    print("-" * 40)
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:<15}: {count:>5} rows")
    
    cursor.close()
    conn.close()
    
except Exception as e:
    print(f"❌ Error: {e}")
    import traceback
    traceback.print_exc()
PYEOF

echo ""
echo "📋 Verify data in PostgreSQL:"
echo "docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c '\dt'"
echo ""
echo "Query examples:"
echo "  SELECT * FROM test_data LIMIT 10;"
echo "  SELECT * FROM employees WHERE department = 'IT';"
echo "  SELECT * FROM products WHERE unit_price > 100;"