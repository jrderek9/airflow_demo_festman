#!/bin/bash

echo "🚀 Quick Data Load to PostgreSQL (v2)"
echo "===================================="
echo ""

# First, let's check if we can connect
echo "1. Testing PostgreSQL connection..."
docker exec airflow-csv-postgres-postgres-1 psql -U airflow -d airflow -c "SELECT version();" || {
    echo "❌ Failed to connect to PostgreSQL"
    exit 1
}

echo ""
echo "2. Loading sample data..."

# Load data using a simpler approach
docker exec -i airflow-csv-postgres-postgres-1 psql -U airflow -d airflow << 'EOF'
-- Create test_data table
DROP TABLE IF EXISTS test_data CASCADE;
CREATE TABLE test_data (
    id SERIAL PRIMARY KEY,
    name VARCHAR(100),
    value DECIMAL(10,2),
    category VARCHAR(50),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Insert test data
INSERT INTO test_data (name, value, category) VALUES
    ('Item 1', 99.99, 'Electronics'),
    ('Item 2', 49.50, 'Clothing'),
    ('Item 3', 199.99, 'Electronics'),
    ('Item 4', 29.99, 'Books'),
    ('Item 5', 149.00, 'Sports'),
    ('Item 6', 79.99, 'Electronics'),
    ('Item 7', 39.99, 'Clothing'),
    ('Item 8', 89.50, 'Sports'),
    ('Item 9', 24.99, 'Books'),
    ('Item 10', 299.99, 'Electronics');

-- Create employees table if not exists
CREATE TABLE IF NOT EXISTS employees (
    employee_id SERIAL PRIMARY KEY,
    first_name VARCHAR(50),
    last_name VARCHAR(50),
    department VARCHAR(50),
    salary DECIMAL(10,2),
    hire_date DATE,
    is_active BOOLEAN DEFAULT TRUE
);

-- Clear and insert sample employees
TRUNCATE TABLE employees RESTART IDENTITY CASCADE;
INSERT INTO employees (first_name, last_name, department, salary, hire_date) VALUES
    ('John', 'Smith', 'IT', 75000, '2023-01-15'),
    ('Jane', 'Doe', 'HR', 65000, '2022-06-20'),
    ('Mike', 'Johnson', 'Sales', 80000, '2021-11-10'),
    ('Sarah', 'Williams', 'IT', 90000, '2020-03-05'),
    ('Tom', 'Brown', 'Finance', 70000, '2023-08-22'),
    ('Emily', 'Davis', 'Marketing', 68000, '2022-12-01'),
    ('David', 'Wilson', 'IT', 85000, '2021-07-18'),
    ('Lisa', 'Anderson', 'HR', 62000, '2023-02-28'),
    ('James', 'Taylor', 'Sales', 95000, '2019-09-12'),
    ('Amy', 'Martinez', 'Finance', 72000, '2022-04-15');

-- Create products table
CREATE TABLE IF NOT EXISTS products (
    product_id VARCHAR(20) PRIMARY KEY,
    product_name VARCHAR(200),
    category VARCHAR(50),
    unit_price DECIMAL(10,2),
    quantity_in_stock INTEGER,
    is_active BOOLEAN DEFAULT TRUE
);

-- Insert sample products
TRUNCATE TABLE products RESTART IDENTITY CASCADE;
INSERT INTO products VALUES
    ('PROD0001', 'Laptop Pro 15', 'Electronics', 1299.99, 50, true),
    ('PROD0002', 'Wireless Mouse', 'Electronics', 29.99, 200, true),
    ('PROD0003', 'Office Chair', 'Furniture', 399.99, 30, true),
    ('PROD0004', 'USB-C Cable', 'Electronics', 19.99, 500, true),
    ('PROD0005', 'Monitor 27"', 'Electronics', 349.99, 75, true),
    ('PROD0006', 'Desk Lamp', 'Furniture', 49.99, 100, true),
    ('PROD0007', 'Keyboard Mechanical', 'Electronics', 129.99, 80, true),
    ('PROD0008', 'Standing Desk', 'Furniture', 599.99, 20, true),
    ('PROD0009', 'Webcam HD', 'Electronics', 79.99, 150, true),
    ('PROD0010', 'Headphones Wireless', 'Electronics', 199.99, 60, true);

-- Show summary
SELECT '=== DATA LOADED SUCCESSFULLY ===' as status;
SELECT 'test_data' as table_name, COUNT(*) as row_count FROM test_data
UNION ALL
SELECT 'employees', COUNT(*) FROM employees
UNION ALL
SELECT 'products', COUNT(*) FROM products
ORDER BY table_name;

-- Show sample data
SELECT '=== SAMPLE EMPLOYEE DATA ===' as info;
SELECT * FROM employees WHERE department = 'IT' ORDER BY salary DESC LIMIT 3;

SELECT '=== SAMPLE PRODUCT DATA ===' as info;
SELECT * FROM products WHERE category = 'Electronics' AND unit_price > 100 ORDER BY unit_price DESC LIMIT 3;
EOF

echo ""
echo "3. Creating the sample_table for simple_csv_to_postgres_dag..."
docker exec -i airflow-csv-postgres-postgres-1 psql -U airflow -d airflow << 'EOF'
-- This table is used by simple_csv_to_postgres_dag
CREATE TABLE IF NOT EXISTS sample_table (
    id SERIAL PRIMARY KEY,
    name VARCHAR(50),
    age INT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Clear and insert data
TRUNCATE TABLE sample_table RESTART IDENTITY CASCADE;
INSERT INTO sample_table (id, name, age) VALUES
    (1, 'John Doe', 30),
    (2, 'Jane Smith', 25),
    (3, 'Bob Johnson', 35),
    (4, 'Alice Williams', 28),
    (5, 'Charlie Brown', 32);

SELECT 'sample_table created with' as status, COUNT(*) as rows FROM sample_table;
EOF

echo ""
echo "✅ Data loading complete!"
echo ""
echo "📊 Quick verification commands:"
echo "   docker exec -it airflow-csv-postgres-postgres-1 psql -U airflow -d airflow"
echo ""
echo "   Once connected, try:"
echo "   \\dt                              -- List all tables"
echo "   SELECT * FROM test_data;"
echo "   SELECT * FROM employees;"
echo "   SELECT * FROM products;"
echo "   \\q                               -- Quit"