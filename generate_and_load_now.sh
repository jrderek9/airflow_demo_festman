#!/bin/bash

# Generate and Load Data Now - Creates sample data and loads it immediately
echo "🚀 GENERATE AND LOAD DATA NOW"
echo "============================="
echo "This will create sample data and load it into your database"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m'

echo -e "${YELLOW}Generating and loading data directly in PostgreSQL...${NC}"

# Run everything in one Python script inside the container
docker exec airflow-csv-postgres-airflow-scheduler-1 python3 << 'EOF'
import psycopg2
import random
from datetime import datetime, timedelta

print("Connecting to PostgreSQL...")
conn = psycopg2.connect(
    host="postgres",
    database="airflow",
    user="airflow",
    password="airflow"
)
cursor = conn.cursor()
print("✅ Connected!")

# 1. Load Employees
print("\n📊 Loading Employees...")
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

# Clear existing data
cursor.execute("TRUNCATE TABLE employees CASCADE;")

# Generate and insert employees
first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa', 'James', 'Mary']
last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis']
departments = ['IT', 'HR', 'Sales', 'Marketing', 'Finance', 'Operations']
job_titles = ['Manager', 'Analyst', 'Developer', 'Specialist', 'Coordinator', 'Director']
cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix']
education_levels = ['High School', 'Bachelor', 'Master', 'PhD']

print("Inserting employees...")
for i in range(1, 10001):  # 10,000 employees
    values = (
        i,  # employee_id
        random.choice(first_names),
        random.choice(last_names),
        random.randint(22, 65),  # age
        random.choice(['M', 'F']),
        f'emp{i}@company.com',
        f'555-{random.randint(1000, 9999)}',
        f'{random.randint(1, 999)} Main St',
        random.choice(cities),
        random.choice(['NY', 'CA', 'IL', 'TX', 'AZ']),
        str(random.randint(10000, 99999)),
        'USA',
        (datetime.now() - timedelta(days=random.randint(0, 3650))).date(),
        random.choice(departments),
        random.choice(job_titles),
        random.randint(30000, 150000),
        round(random.uniform(0, 20), 2),
        random.randint(0, 30),
        random.choice(education_levels),
        round(random.uniform(1, 5), 2),
        round(random.uniform(1, 10), 2),
        round(random.uniform(85, 100), 2),
        random.randint(0, 100),
        random.randint(0, 5),
        random.choice([True, False]),
        random.randint(0, 20),
        random.randint(0, 5),
        random.choice(['Single', 'Married', 'Divorced']),
        random.randint(0, 4),
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
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, values)
    
    if i % 1000 == 0:
        conn.commit()
        print(f"  Inserted {i} employees...")

conn.commit()
print("✅ Loaded 10,000 employees!")

# 2. Load Products
print("\n📊 Loading Products...")
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

cursor.execute("TRUNCATE TABLE products CASCADE;")

categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys']
brands = ['Brand A', 'Brand B', 'Brand C', 'Brand D', 'Brand E']
suppliers = ['Supplier 1', 'Supplier 2', 'Supplier 3']
conditions = ['New', 'Refurbished', 'Used']

print("Inserting products...")
for i in range(1, 5001):  # 5,000 products
    values = (
        f'PROD{i:06d}',
        f'SKU{i:06d}',
        f'Product {i}',
        random.choice(categories),
        random.choice(brands),
        f'Description for product {i}',
        round(random.uniform(10, 1000), 2),
        round(random.uniform(5, 500), 2),
        random.randint(0, 1000),
        random.randint(10, 100),
        random.randint(50, 200),
        f'{random.choice(["A", "B", "C"])}{random.randint(1, 9)}',
        random.choice(suppliers),
        round(random.uniform(0.1, 50), 2),
        f'{random.randint(10, 100)}x{random.randint(10, 100)}x{random.randint(10, 100)}',
        random.choice(conditions),
        (datetime.now() - timedelta(days=random.randint(0, 365))).date(),
        '',
        (datetime.now() - timedelta(days=random.randint(0, 30))).date(),
        random.randint(0, 100),
        random.randint(0, 300),
        random.randint(0, 1000),
        round(random.uniform(1, 5), 1),
        random.randint(0, 1000),
        True,
        round(random.uniform(0, 50), 1),
        round(random.uniform(0, 20), 2),
        random.choice(['Standard', 'Express', 'Overnight']),
        random.choice(['USA', 'China', 'Germany', 'Japan']),
        f'BAR{i:09d}',
        0
    )
    
    cursor.execute("""
        INSERT INTO products VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    """, values)
    
    if i % 1000 == 0:
        conn.commit()
        print(f"  Inserted {i} products...")

conn.commit()
print("✅ Loaded 5,000 products!")

# 3. Load Sample Table
print("\n📊 Loading Sample Table...")
cursor.execute("""
    CREATE TABLE IF NOT EXISTS sample_table (
        id INTEGER PRIMARY KEY,
        name VARCHAR(50),
        age INTEGER,
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );
""")

cursor.execute("TRUNCATE TABLE sample_table;")

sample_data = [
    (1, 'John Doe', 30),
    (2, 'Jane Smith', 25),
    (3, 'Bob Johnson', 35),
    (4, 'Alice Williams', 28),
    (5, 'Charlie Brown', 32)
]

for id, name, age in sample_data:
    cursor.execute("INSERT INTO sample_table (id, name, age) VALUES (%s, %s, %s)", (id, name, age))

conn.commit()
print("✅ Loaded 5 rows into sample_table!")

# 4. Create empty tables for other data
print("\n📊 Creating other tables...")
other_tables = [
    """CREATE TABLE IF NOT EXISTS sales_transactions (
        transaction_id VARCHAR(20) PRIMARY KEY,
        order_date TIMESTAMP,
        customer_id VARCHAR(20),
        total_amount DECIMAL(12,2),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );""",
    
    """CREATE TABLE IF NOT EXISTS support_tickets (
        ticket_id VARCHAR(20) PRIMARY KEY,
        created_date TIMESTAMP,
        customer_id VARCHAR(20),
        status VARCHAR(20),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );""",
    
    """CREATE TABLE IF NOT EXISTS web_analytics (
        session_id VARCHAR(30) PRIMARY KEY,
        user_id VARCHAR(20),
        timestamp TIMESTAMP,
        page_url VARCHAR(200),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );""",
    
    """CREATE TABLE IF NOT EXISTS healthcare_records (
        patient_id VARCHAR(20) PRIMARY KEY,
        admission_date DATE,
        patient_name VARCHAR(100),
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
    );"""
]

for create_sql in other_tables:
    try:
        cursor.execute(create_sql)
        conn.commit()
    except Exception as e:
        if "already exists" not in str(e):
            print(f"Error: {e}")

# 5. Show final summary
print("\n" + "="*50)
print("📊 FINAL SUMMARY")
print("="*50)

tables = ['employees', 'products', 'sample_table', 'sales_transactions', 'support_tickets', 'web_analytics', 'healthcare_records']
total_rows = 0

for table in tables:
    cursor.execute(f"SELECT COUNT(*) FROM {table}")
    count = cursor.fetchone()[0]
    print(f"{table:<20}: {count:>10,} rows")
    total_rows += count

print(f"\nTOTAL ROWS: {total_rows:,}")

# Show sample data
print("\n📋 Sample Data from employees:")
cursor.execute("SELECT employee_id, first_name, last_name, department, salary FROM employees LIMIT 5")
for row in cursor.fetchall():
    print(f"  ID: {row[0]}, Name: {row[1]} {row[2]}, Dept: {row[3]}, Salary: ${row[4]:,.2f}")

print("\n📋 Sample Data from products:")
cursor.execute("SELECT product_id, product_name, category, unit_price FROM products LIMIT 5")
for row in cursor.fetchall():
    print(f"  {row[0]}: {row[1]} ({row[2]}) - ${row[3]:.2f}")

cursor.close()
conn.close()
print("\n✅ ALL DONE!")
EOF

echo ""
echo -e "${GREEN}🎉 SUCCESS!${NC}"
echo "==========="
echo ""
echo "✅ Generated and loaded sample data:"
echo "   - 10,000 employees"
echo "   - 5,000 products"
echo "   - 5 sample records"
echo "   - Empty tables created for other data"
echo ""
echo "📊 To verify your data:"
echo "   docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c 'SELECT COUNT(*) FROM employees;'"
echo ""
echo "🔍 To explore the data:"
echo "   docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow"
echo "   Then try: SELECT * FROM employees LIMIT 10;"
echo ""
echo "🌐 Access points:"
echo "   - Airflow UI: http://localhost:8080 (admin/admin)"
echo "   - pgAdmin: http://localhost:5050 (admin@pgadmin.com/admin)"
echo ""
echo "💡 Your database now has 15,005 rows of data to work with!"