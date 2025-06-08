#!/bin/bash

# Debug and Load Data - Shows exactly what's happening
echo "🔍 DEBUG AND LOAD DATA"
echo "====================="
echo ""

# First, let's check what's actually happening
echo "1. Checking PostgreSQL connection..."
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT version();"

echo -e "\n2. Checking existing tables..."
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "\dt" | grep -E "(employees|products|sample_table)"

echo -e "\n3. Loading data with full error reporting..."
docker exec airflow-csv-postgres-airflow-scheduler-1 python3 << 'EOF'
import sys
import traceback

try:
    import psycopg2
    print("✅ psycopg2 imported successfully")
except Exception as e:
    print(f"❌ Failed to import psycopg2: {e}")
    sys.exit(1)

try:
    # Connect to database
    print("\nConnecting to PostgreSQL...")
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL!")
    
    # First, let's insert data into sample_table as a test
    print("\n📊 Testing with sample_table...")
    
    # Create table if not exists
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS sample_table (
            id INTEGER PRIMARY KEY,
            name VARCHAR(50),
            age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    print("✅ Table created/verified")
    
    # Clear existing data
    cursor.execute("DELETE FROM sample_table;")
    conn.commit()
    print("✅ Cleared existing data")
    
    # Insert test data
    test_data = [
        (1, 'Test User 1', 25),
        (2, 'Test User 2', 30),
        (3, 'Test User 3', 35),
        (4, 'Test User 4', 40),
        (5, 'Test User 5', 45)
    ]
    
    for row in test_data:
        cursor.execute(
            "INSERT INTO sample_table (id, name, age) VALUES (%s, %s, %s)",
            row
        )
    
    conn.commit()
    print(f"✅ Inserted {len(test_data)} rows")
    
    # Verify
    cursor.execute("SELECT COUNT(*) FROM sample_table")
    count = cursor.fetchone()[0]
    print(f"✅ Verified: {count} rows in sample_table")
    
    # Now load employees
    print("\n📊 Loading employees table...")
    
    # Create employees table
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS employees (
            employee_id INTEGER PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            age INTEGER,
            gender VARCHAR(10),
            email VARCHAR(100),
            phone VARCHAR(20),
            address VARCHAR(200),
            city VARCHAR(50),
            state VARCHAR(2),
            zip_code VARCHAR(10),
            country VARCHAR(50),
            hire_date DATE,
            department VARCHAR(50),
            job_title VARCHAR(50),
            salary DECIMAL(10,2),
            bonus_percentage DECIMAL(5,2),
            years_experience INTEGER,
            education_level VARCHAR(50),
            performance_rating DECIMAL(3,2),
            satisfaction_score DECIMAL(3,2),
            attendance_rate DECIMAL(5,2),
            training_hours INTEGER,
            certifications INTEGER,
            is_manager BOOLEAN,
            team_size INTEGER,
            remote_work_days INTEGER,
            marital_status VARCHAR(20),
            dependents INTEGER,
            emergency_contact VARCHAR(100),
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            is_active BOOLEAN DEFAULT TRUE
        );
    """)
    conn.commit()
    print("✅ Employees table created/verified")
    
    # Clear existing data
    cursor.execute("DELETE FROM employees;")
    conn.commit()
    
    # Insert employees - just 100 for testing
    import random
    from datetime import datetime, timedelta
    
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones']
    departments = ['IT', 'HR', 'Sales', 'Marketing']
    
    print("Inserting employees...")
    for i in range(1, 101):  # Just 100 for testing
        employee = (
            i,  # employee_id
            random.choice(first_names),
            random.choice(last_names),
            random.randint(25, 65),  # age
            random.choice(['M', 'F']),
            f'employee{i}@company.com',
            f'555-{random.randint(1000,9999)}',
            f'{random.randint(100,999)} Main St',
            'New York',
            'NY',
            '10001',
            'USA',
            (datetime.now() - timedelta(days=random.randint(1, 1000))).date(),
            random.choice(departments),
            'Developer',
            random.randint(50000, 150000),
            random.uniform(5, 20),
            random.randint(1, 20),
            'Bachelor',
            random.uniform(3, 5),
            random.uniform(6, 10),
            random.uniform(90, 100),
            random.randint(10, 100),
            random.randint(0, 5),
            random.choice([True, False]),
            random.randint(0, 10),
            random.randint(0, 5),
            'Single',
            0,
            'Emergency Contact'
        )
        
        cursor.execute("""
            INSERT INTO employees (
                employee_id, first_name, last_name, age, gender, email, phone, address,
                city, state, zip_code, country, hire_date, department, job_title, salary,
                bonus_percentage, years_experience, education_level, performance_rating,
                satisfaction_score, attendance_rate, training_hours, certifications,
                is_manager, team_size, remote_work_days, marital_status, dependents,
                emergency_contact
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, employee)
        
        if i % 10 == 0:
            print(f"  Inserted {i} employees...")
    
    conn.commit()
    print("✅ Employees inserted!")
    
    # Verify employees
    cursor.execute("SELECT COUNT(*) FROM employees")
    emp_count = cursor.fetchone()[0]
    print(f"✅ Verified: {emp_count} employees in database")
    
    # Create and load products
    print("\n📊 Loading products table...")
    cursor.execute("""
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(20) PRIMARY KEY,
            sku VARCHAR(20),
            product_name VARCHAR(200),
            category VARCHAR(50),
            brand VARCHAR(50),
            description TEXT,
            unit_price DECIMAL(10,2),
            cost_price DECIMAL(10,2),
            quantity_in_stock INTEGER,
            reorder_level INTEGER,
            reorder_quantity INTEGER,
            warehouse_location VARCHAR(50),
            supplier VARCHAR(50),
            weight_kg DECIMAL(10,2),
            dimensions_cm VARCHAR(50),
            condition VARCHAR(50),
            manufactured_date DATE,
            expiry_date VARCHAR(20),
            last_restocked DATE,
            units_sold_30days INTEGER,
            units_sold_90days INTEGER,
            units_sold_365days INTEGER,
            rating DECIMAL(3,2),
            review_count INTEGER,
            is_active BOOLEAN,
            discount_percentage DECIMAL(5,2),
            tax_rate DECIMAL(5,2),
            shipping_class VARCHAR(50),
            country_of_origin VARCHAR(50),
            barcode VARCHAR(50),
            minimum_age INTEGER,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)
    conn.commit()
    
    # Clear and insert products
    cursor.execute("DELETE FROM products;")
    
    for i in range(1, 51):  # Just 50 products for testing
        product = (
            f'PROD{i:06d}',
            f'SKU{i:06d}',
            f'Product {i}',
            'Electronics',
            'Brand A',
            f'Description for product {i}',
            random.uniform(10, 500),
            random.uniform(5, 250),
            random.randint(10, 1000),
            50,
            100,
            'Warehouse A',
            'Supplier 1',
            random.uniform(0.1, 10),
            '10x10x10',
            'New',
            datetime.now().date(),
            '',
            datetime.now().date(),
            random.randint(0, 50),
            random.randint(0, 150),
            random.randint(0, 500),
            random.uniform(3, 5),
            random.randint(0, 100),
            True,
            random.uniform(0, 30),
            8.5,
            'Standard',
            'USA',
            f'BAR{i:09d}',
            0
        )
        
        cursor.execute("""
            INSERT INTO products VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
        """, product)
    
    conn.commit()
    print("✅ Products inserted!")
    
    # Verify products
    cursor.execute("SELECT COUNT(*) FROM products")
    prod_count = cursor.fetchone()[0]
    print(f"✅ Verified: {prod_count} products in database")
    
    # Show summary
    print("\n" + "="*50)
    print("📊 SUMMARY")
    print("="*50)
    
    tables = ['sample_table', 'employees', 'products']
    for table in tables:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"{table:<15}: {count:>5} rows")
    
    # Show sample data
    print("\n📋 Sample data from employees:")
    cursor.execute("SELECT employee_id, first_name, last_name, department, salary FROM employees LIMIT 5")
    for row in cursor.fetchall():
        print(f"  ID: {row[0]}, {row[1]} {row[2]}, {row[3]}, ${row[4]:,.2f}")
    
    cursor.close()
    conn.close()
    print("\n✅ Data loading complete!")
    
except Exception as e:
    print(f"\n❌ ERROR: {e}")
    print("\nFull traceback:")
    traceback.print_exc()
    sys.exit(1)
EOF

echo -e "\n4. Verifying data was loaded..."
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow << 'EOF'
SELECT '=== DATA VERIFICATION ===' as info;
SELECT 'sample_table' as table_name, COUNT(*) as rows FROM sample_table
UNION ALL
SELECT 'employees', COUNT(*) FROM employees
UNION ALL
SELECT 'products', COUNT(*) FROM products
ORDER BY table_name;

SELECT '=== SAMPLE EMPLOYEE DATA ===' as info;
SELECT employee_id, first_name, last_name, department, salary 
FROM employees 
LIMIT 5;

SELECT '=== SAMPLE PRODUCT DATA ===' as info;
SELECT product_id, product_name, category, unit_price 
FROM products 
LIMIT 5;
EOF

echo ""
echo "✅ COMPLETE!"
echo ""
echo "If you see data above, it worked! If not, check the error messages."
echo ""
echo "📊 To interact with your data:"
echo "   docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow"
echo ""
echo "🔍 Useful queries:"
echo "   \\dt                    -- List all tables"
echo "   SELECT * FROM employees LIMIT 10;"
echo "   SELECT COUNT(*) FROM products;"
echo "   \\q                     -- Quit psql"