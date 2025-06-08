#!/bin/bash

# Fix and Load Data - Ensures CSV files are accessible and loads all data
echo "🔧 FIX AND LOAD ALL DATA"
echo "========================"
echo ""

# Colors
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
RED='\033[0;31m'
NC='\033[0m'

# Step 1: Check if sample_files exist locally
echo -e "${YELLOW}Step 1: Checking for CSV files locally...${NC}"
if [ -d "sample_files" ]; then
    echo "Found sample_files directory"
    ls -lh sample_files/*.csv 2>/dev/null | head -5
else
    echo -e "${RED}sample_files directory not found locally!${NC}"
    echo "Creating sample data..."
    mkdir -p sample_files
fi

# Step 2: Copy sample files to containers
echo -e "\n${YELLOW}Step 2: Copying CSV files to containers...${NC}"
docker cp sample_files airflow-csv-postgres-airflow-scheduler-1:/opt/airflow/
docker cp sample_files airflow-csv-postgres-airflow-webserver-1:/opt/airflow/

# Verify files were copied
echo "Verifying files in scheduler container:"
docker exec airflow-csv-postgres-airflow-scheduler-1 ls -la /opt/airflow/sample_files/*.csv 2>/dev/null | wc -l

# Step 3: If no CSV files exist, generate them
csv_count=$(docker exec airflow-csv-postgres-airflow-scheduler-1 ls /opt/airflow/sample_files/*.csv 2>/dev/null | wc -l)
if [ "$csv_count" -eq 0 ]; then
    echo -e "\n${YELLOW}Step 3: No CSV files found. Generating sample data...${NC}"
    
    # Generate data inside the container
    docker exec airflow-csv-postgres-airflow-scheduler-1 bash -c '
        cd /opt/airflow
        mkdir -p sample_files
        cd sample_files
        
        # Generate the script
        cat > generate_data.py << "EOFPY"
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random

print("Generating sample CSV files...")

# 1. Generate employees (1000 rows for demo)
print("Generating employees...")
employees = {
    "employee_id": range(1, 1001),
    "first_name": [random.choice(["John", "Jane", "Mike", "Sarah", "David"]) for _ in range(1000)],
    "last_name": [random.choice(["Smith", "Johnson", "Williams", "Brown"]) for _ in range(1000)],
    "age": np.random.randint(22, 65, 1000),
    "gender": np.random.choice(["M", "F"], 1000),
    "email": [f"emp{i}@company.com" for i in range(1, 1001)],
    "phone": [f"555-{random.randint(1000, 9999)}" for _ in range(1000)],
    "address": [f"{random.randint(1, 999)} Main St" for _ in range(1000)],
    "city": np.random.choice(["New York", "Los Angeles", "Chicago"], 1000),
    "state": np.random.choice(["NY", "CA", "IL"], 1000),
    "zip_code": [str(random.randint(10000, 99999)) for _ in range(1000)],
    "country": ["USA"] * 1000,
    "hire_date": [(datetime.now() - timedelta(days=random.randint(0, 3650))).strftime("%Y-%m-%d") for _ in range(1000)],
    "department": np.random.choice(["IT", "HR", "Sales", "Marketing"], 1000),
    "job_title": np.random.choice(["Manager", "Analyst", "Developer"], 1000),
    "salary": np.random.randint(30000, 150000, 1000),
    "bonus_percentage": np.random.uniform(0, 20, 1000),
    "years_experience": np.random.randint(0, 30, 1000),
    "education_level": np.random.choice(["Bachelor", "Master", "PhD"], 1000),
    "performance_rating": np.random.uniform(1, 5, 1000),
    "satisfaction_score": np.random.uniform(1, 10, 1000),
    "attendance_rate": np.random.uniform(85, 100, 1000),
    "training_hours": np.random.randint(0, 100, 1000),
    "certifications": np.random.randint(0, 5, 1000),
    "is_manager": np.random.choice([True, False], 1000),
    "team_size": np.random.randint(0, 20, 1000),
    "remote_work_days": np.random.randint(0, 5, 1000),
    "marital_status": np.random.choice(["Single", "Married"], 1000),
    "dependents": np.random.randint(0, 4, 1000),
    "emergency_contact": ["Emergency Contact"] * 1000
}
pd.DataFrame(employees).to_csv("employees_large.csv", index=False)

# 2. Generate products (500 rows)
print("Generating products...")
products = {
    "product_id": [f"PROD{i:06d}" for i in range(1, 501)],
    "sku": [f"SKU{i:06d}" for i in range(1, 501)],
    "product_name": [f"Product {i}" for i in range(1, 501)],
    "category": np.random.choice(["Electronics", "Clothing", "Home"], 500),
    "brand": np.random.choice(["Brand A", "Brand B", "Brand C"], 500),
    "description": ["Product description"] * 500,
    "unit_price": np.random.uniform(10, 1000, 500),
    "cost_price": np.random.uniform(5, 500, 500),
    "quantity_in_stock": np.random.randint(0, 1000, 500),
    "reorder_level": np.random.randint(10, 100, 500),
    "reorder_quantity": np.random.randint(50, 200, 500),
    "warehouse_location": np.random.choice(["A1", "B2", "C3"], 500),
    "supplier": np.random.choice(["Supplier 1", "Supplier 2"], 500),
    "weight_kg": np.random.uniform(0.1, 50, 500),
    "dimensions_cm": ["10x10x10"] * 500,
    "condition": ["New"] * 500,
    "manufactured_date": [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime("%Y-%m-%d") for _ in range(500)],
    "expiry_date": [""] * 500,
    "last_restocked": [(datetime.now() - timedelta(days=random.randint(0, 30))).strftime("%Y-%m-%d") for _ in range(500)],
    "units_sold_30days": np.random.randint(0, 100, 500),
    "units_sold_90days": np.random.randint(0, 300, 500),
    "units_sold_365days": np.random.randint(0, 1000, 500),
    "rating": np.random.uniform(1, 5, 500),
    "review_count": np.random.randint(0, 1000, 500),
    "is_active": [True] * 500,
    "discount_percentage": np.random.uniform(0, 50, 500),
    "tax_rate": np.random.uniform(0, 20, 500),
    "shipping_class": np.random.choice(["Standard", "Express"], 500),
    "country_of_origin": np.random.choice(["USA", "China", "Germany"], 500),
    "barcode": [f"BAR{i:09d}" for i in range(1, 501)],
    "minimum_age": [0] * 500
}
pd.DataFrame(products).to_csv("product_inventory.csv", index=False)

# 3. Generate simple input.csv
print("Generating input.csv...")
input_data = {
    "id": [1, 2, 3, 4, 5],
    "name": ["John Doe", "Jane Smith", "Bob Johnson", "Alice Williams", "Charlie Brown"],
    "age": [30, 25, 35, 28, 32]
}
pd.DataFrame(input_data).to_csv("input.csv", index=False)

print("✅ Generated 3 CSV files!")
EOFPY

        # Run the generation script
        python3 generate_data.py
        
        # List generated files
        echo "Generated files:"
        ls -lh *.csv
    '
else
    echo -e "${GREEN}✅ Found $csv_count CSV files${NC}"
fi

# Step 4: Load data into PostgreSQL
echo -e "\n${YELLOW}Step 4: Loading data into PostgreSQL...${NC}"

docker exec airflow-csv-postgres-airflow-scheduler-1 python3 << 'EOF'
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values
import os

print("Starting data load process...")

# Database connection
try:
    conn = psycopg2.connect(
        host="postgres",
        database="airflow",
        user="airflow",
        password="airflow"
    )
    cursor = conn.cursor()
    print("✅ Connected to PostgreSQL")
except Exception as e:
    print(f"❌ Failed to connect to database: {e}")
    exit(1)

# Check what CSV files we have
csv_dir = "/opt/airflow/sample_files"
csv_files = [f for f in os.listdir(csv_dir) if f.endswith('.csv')]
print(f"\nFound {len(csv_files)} CSV files: {csv_files}")

# Load employees data
if "employees_large.csv" in csv_files:
    print("\n📊 Loading employees data...")
    try:
        df = pd.read_csv(f"{csv_dir}/employees_large.csv")
        print(f"  Read {len(df)} rows from employees_large.csv")
        
        # Create table if it doesn't exist
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
        cursor.execute("TRUNCATE TABLE employees;")
        
        # Insert data
        columns = df.columns.tolist()
        # Add missing columns with defaults
        if 'created_at' not in columns:
            df['created_at'] = pd.Timestamp.now()
        if 'updated_at' not in columns:
            df['updated_at'] = pd.Timestamp.now()
        if 'is_active' not in columns:
            df['is_active'] = True
            
        # Prepare data
        data = df.to_dict('records')
        
        # Insert in chunks
        chunk_size = 100
        for i in range(0, len(data), chunk_size):
            chunk = data[i:i+chunk_size]
            values = [[row.get(col) for col in columns] for row in chunk]
            
            insert_query = f"""
                INSERT INTO employees ({', '.join(columns)})
                VALUES ({', '.join(['%s'] * len(columns))})
                ON CONFLICT (employee_id) DO NOTHING
            """
            
            for value in values:
                cursor.execute(insert_query, value)
        
        conn.commit()
        print(f"✅ Loaded {len(df)} employees")
        
    except Exception as e:
        print(f"❌ Error loading employees: {e}")
        conn.rollback()

# Load products data
if "product_inventory.csv" in csv_files:
    print("\n📊 Loading products data...")
    try:
        df = pd.read_csv(f"{csv_dir}/product_inventory.csv")
        print(f"  Read {len(df)} rows from product_inventory.csv")
        
        # Create table
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
        
        # Clear and insert
        cursor.execute("TRUNCATE TABLE products;")
        
        # Add missing columns
        if 'created_at' not in df.columns:
            df['created_at'] = pd.Timestamp.now()
        if 'updated_at' not in df.columns:
            df['updated_at'] = pd.Timestamp.now()
        
        # Insert data
        for _, row in df.iterrows():
            values = tuple(row.get(col) for col in df.columns)
            placeholders = ', '.join(['%s'] * len(df.columns))
            insert_query = f"INSERT INTO products ({', '.join(df.columns)}) VALUES ({placeholders}) ON CONFLICT (product_id) DO NOTHING"
            cursor.execute(insert_query, values)
        
        conn.commit()
        print(f"✅ Loaded {len(df)} products")
        
    except Exception as e:
        print(f"❌ Error loading products: {e}")
        conn.rollback()

# Load sample_table data
if "input.csv" in csv_files:
    print("\n📊 Loading sample table data...")
    try:
        df = pd.read_csv(f"{csv_dir}/input.csv")
        
        # Create table
        cursor.execute("""
            CREATE TABLE IF NOT EXISTS sample_table (
                id INTEGER PRIMARY KEY,
                name VARCHAR(50),
                age INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
        """)
        
        # Clear and insert
        cursor.execute("TRUNCATE TABLE sample_table;")
        
        for _, row in df.iterrows():
            cursor.execute(
                "INSERT INTO sample_table (id, name, age) VALUES (%s, %s, %s)",
                (row['id'], row['name'], row['age'])
            )
        
        conn.commit()
        print(f"✅ Loaded {len(df)} rows into sample_table")
        
    except Exception as e:
        print(f"❌ Error loading sample_table: {e}")
        conn.rollback()

# Create empty tables for missing CSVs
print("\n📊 Creating empty tables for missing data...")
empty_tables = {
    'sales_transactions': """
        CREATE TABLE IF NOT EXISTS sales_transactions (
            transaction_id VARCHAR(20) PRIMARY KEY,
            order_date TIMESTAMP,
            customer_id VARCHAR(20),
            customer_name VARCHAR(100),
            customer_email VARCHAR(100),
            customer_type VARCHAR(20),
            product_ids TEXT,
            product_names TEXT,
            quantities TEXT,
            unit_prices TEXT,
            subtotal DECIMAL(12,2),
            tax_amount DECIMAL(10,2),
            shipping_cost DECIMAL(10,2),
            discount_amount DECIMAL(10,2),
            total_amount DECIMAL(12,2),
            payment_method VARCHAR(50),
            payment_status VARCHAR(20),
            order_status VARCHAR(20),
            sales_channel VARCHAR(50),
            sales_rep_id VARCHAR(10),
            sales_rep_name VARCHAR(100),
            shipping_address TEXT,
            billing_address TEXT,
            region VARCHAR(50),
            country VARCHAR(50),
            currency VARCHAR(10),
            exchange_rate DECIMAL(10,4),
            notes TEXT,
            refund_amount DECIMAL(12,2) DEFAULT 0,
            is_gift BOOLEAN DEFAULT FALSE,
            loyalty_points_earned INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    'support_tickets': """
        CREATE TABLE IF NOT EXISTS support_tickets (
            ticket_id VARCHAR(20) PRIMARY KEY,
            created_date TIMESTAMP,
            customer_id VARCHAR(20),
            customer_name VARCHAR(100),
            customer_email VARCHAR(100),
            category VARCHAR(50),
            subcategory VARCHAR(100),
            priority VARCHAR(20),
            status VARCHAR(20),
            subject VARCHAR(200),
            description TEXT,
            channel VARCHAR(50),
            assigned_to VARCHAR(10),
            department VARCHAR(50),
            first_response_time_hours DECIMAL(10,2),
            resolution_time_hours DECIMAL(10,2),
            number_of_interactions INTEGER,
            satisfaction_rating DECIMAL(3,2),
            tags TEXT,
            related_order_id VARCHAR(20),
            product_id VARCHAR(20),
            escalated BOOLEAN DEFAULT FALSE,
            reopened BOOLEAN DEFAULT FALSE,
            agent_notes TEXT,
            resolution_notes TEXT,
            sla_breach BOOLEAN DEFAULT FALSE,
            response_sla_hours INTEGER,
            resolution_sla_hours INTEGER,
            customer_lifetime_value DECIMAL(12,2),
            is_vip_customer BOOLEAN DEFAULT FALSE,
            language VARCHAR(20),
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """,
    'web_analytics': """
        CREATE TABLE IF NOT EXISTS web_analytics (
            session_id VARCHAR(30) PRIMARY KEY,
            user_id VARCHAR(20),
            timestamp TIMESTAMP,
            page_url VARCHAR(200),
            referrer_url VARCHAR(500),
            source VARCHAR(50),
            medium VARCHAR(50),
            campaign VARCHAR(100),
            device_type VARCHAR(20),
            browser VARCHAR(50),
            operating_system VARCHAR(50),
            screen_resolution VARCHAR(20),
            country VARCHAR(50),
            city VARCHAR(50),
            language VARCHAR(20),
            session_duration_seconds INTEGER,
            pages_viewed INTEGER,
            bounce BOOLEAN DEFAULT FALSE,
            conversion BOOLEAN DEFAULT FALSE,
            conversion_value DECIMAL(10,2) DEFAULT 0,
            events_triggered INTEGER DEFAULT 0,
            click_through_rate DECIMAL(5,2),
            scroll_depth_percentage INTEGER,
            form_submissions INTEGER DEFAULT 0,
            downloads INTEGER DEFAULT 0,
            video_plays INTEGER DEFAULT 0,
            add_to_cart INTEGER DEFAULT 0,
            purchases INTEGER DEFAULT 0,
            new_vs_returning VARCHAR(20),
            user_agent TEXT,
            ip_address_hash VARCHAR(64),
            session_quality_score DECIMAL(5,2)
        );
    """,
    'healthcare_records': """
        CREATE TABLE IF NOT EXISTS healthcare_records (
            patient_id VARCHAR(20) PRIMARY KEY,
            admission_date DATE,
            discharge_date DATE,
            patient_name VARCHAR(100),
            date_of_birth DATE,
            age INTEGER,
            gender VARCHAR(20),
            blood_type VARCHAR(5),
            height_cm INTEGER,
            weight_kg DECIMAL(5,2),
            bmi DECIMAL(4,2),
            department VARCHAR(50),
            primary_diagnosis VARCHAR(200),
            secondary_diagnosis VARCHAR(200),
            attending_physician VARCHAR(100),
            referring_physician VARCHAR(100),
            procedures_performed TEXT,
            medications_prescribed TEXT,
            lab_results TEXT,
            vital_signs TEXT,
            insurance_provider VARCHAR(50),
            insurance_id VARCHAR(20),
            copay_amount DECIMAL(10,2),
            total_charges DECIMAL(12,2),
            insurance_covered DECIMAL(12,2),
            patient_balance DECIMAL(12,2),
            admission_type VARCHAR(20),
            discharge_disposition VARCHAR(50),
            length_of_stay_days INTEGER,
            readmission_risk VARCHAR(20),
            patient_satisfaction DECIMAL(3,2),
            follow_up_required BOOLEAN DEFAULT TRUE,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """
}

for table_name, create_sql in empty_tables.items():
    try:
        cursor.execute(create_sql)
        conn.commit()
        print(f"✅ Created table: {table_name}")
    except Exception as e:
        if "already exists" in str(e):
            print(f"  Table {table_name} already exists")
        else:
            print(f"❌ Error creating {table_name}: {e}")

# Show final counts
print("\n📊 Final Data Summary:")
print("=" * 40)
tables = ['employees', 'products', 'sample_table', 'sales_transactions', 'support_tickets', 'web_analytics', 'healthcare_records']
for table in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        print(f"  {table:<20}: {count:>8,} rows")
    except:
        print(f"  {table:<20}: table not found")

cursor.close()
conn.close()
print("\n✅ Data loading complete!")
EOF

# Step 5: Verify the data
echo -e "\n${YELLOW}Step 5: Verifying data...${NC}"
echo ""
echo "📊 DATABASE SUMMARY:"
echo "==================="
docker exec airflow-csv-postgres-airflow-scheduler-1 python3 << 'EOF'
import psycopg2

conn = psycopg2.connect(host="postgres", database="airflow", user="airflow", password="airflow")
cursor = conn.cursor()

tables = ['employees', 'products', 'sample_table', 'sales_transactions', 'support_tickets', 'web_analytics', 'healthcare_records']
total_rows = 0

for table in tables:
    try:
        cursor.execute(f"SELECT COUNT(*) FROM {table}")
        count = cursor.fetchone()[0]
        if count > 0:
            print(f"✅ {table:<20}: {count:>8,} rows")
            total_rows += count
        else:
            print(f"   {table:<20}: empty")
    except:
        print(f"❌ {table:<20}: not found")

print(f"\nTotal rows in database: {total_rows:,}")
cursor.close()
conn.close()
EOF

echo ""
echo -e "${GREEN}🎉 COMPLETE!${NC}"
echo "============"
echo ""
echo "✅ Data has been loaded into PostgreSQL"
echo "✅ Tables are created and populated"
echo ""
echo "📊 To explore your data:"
echo "  docker exec -it airflow-csv-postgres-airflow-scheduler-1 python3"
echo "  >>> import pandas as pd"
echo "  >>> import psycopg2"
echo "  >>> conn = psycopg2.connect(host='postgres', database='airflow', user='airflow', password='airflow')"
echo "  >>> df = pd.read_sql('SELECT * FROM employees LIMIT 10', conn)"
echo "  >>> print(df)"
echo ""
echo "🌐 Or use pgAdmin: http://localhost:5050"
echo "  Server: postgres"
echo "  Database: airflow"
echo "  Username: airflow"
echo "  Password: airflow"