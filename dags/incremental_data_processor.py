from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import numpy as np
import random
import os
import json
from typing import Dict, List, Any

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 2,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email_on_retry': True,
    'email': ['admin@company.com'],
}

# Configuration for incremental processing
CHUNK_SIZE = 10000
INCREMENTAL_ROWS_PER_RUN = {
    'employees': 1000,
    'products': 500,
    'sales_transactions': 2000,
    'support_tickets': 1500,
    'web_analytics': 3000,
    'healthcare_records': 800
}

# Table definitions with all 30 columns each
TABLE_DEFINITIONS = {
    'employees': """
        CREATE TABLE IF NOT EXISTS employees (
            employee_id SERIAL PRIMARY KEY,
            first_name VARCHAR(50),
            last_name VARCHAR(50),
            age INTEGER,
            gender VARCHAR(10),
            email VARCHAR(100) UNIQUE,
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
        
        CREATE INDEX IF NOT EXISTS idx_employees_department ON employees(department);
        CREATE INDEX IF NOT EXISTS idx_employees_hire_date ON employees(hire_date);
        CREATE INDEX IF NOT EXISTS idx_employees_email ON employees(email);
    """,
    
    'products': """
        CREATE TABLE IF NOT EXISTS products (
            product_id VARCHAR(20) PRIMARY KEY,
            sku VARCHAR(20) UNIQUE,
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
            expiry_date DATE,
            last_restocked TIMESTAMP,
            units_sold_30days INTEGER DEFAULT 0,
            units_sold_90days INTEGER DEFAULT 0,
            units_sold_365days INTEGER DEFAULT 0,
            rating DECIMAL(3,2),
            review_count INTEGER DEFAULT 0,
            is_active BOOLEAN DEFAULT TRUE,
            discount_percentage DECIMAL(5,2) DEFAULT 0,
            tax_rate DECIMAL(5,2),
            shipping_class VARCHAR(50),
            country_of_origin VARCHAR(50),
            barcode VARCHAR(50) UNIQUE,
            minimum_age INTEGER DEFAULT 0,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        
        CREATE INDEX IF NOT EXISTS idx_products_category ON products(category);
        CREATE INDEX IF NOT EXISTS idx_products_brand ON products(brand);
        CREATE INDEX IF NOT EXISTS idx_products_sku ON products(sku);
    """,
    
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
        
        CREATE INDEX IF NOT EXISTS idx_sales_order_date ON sales_transactions(order_date);
        CREATE INDEX IF NOT EXISTS idx_sales_customer ON sales_transactions(customer_id);
        CREATE INDEX IF NOT EXISTS idx_sales_status ON sales_transactions(order_status);
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
        
        CREATE INDEX IF NOT EXISTS idx_tickets_created ON support_tickets(created_date);
        CREATE INDEX IF NOT EXISTS idx_tickets_status ON support_tickets(status);
        CREATE INDEX IF NOT EXISTS idx_tickets_priority ON support_tickets(priority);
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
        
        CREATE INDEX IF NOT EXISTS idx_analytics_timestamp ON web_analytics(timestamp);
        CREATE INDEX IF NOT EXISTS idx_analytics_user ON web_analytics(user_id);
        CREATE INDEX IF NOT EXISTS idx_analytics_page ON web_analytics(page_url);
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
        
        CREATE INDEX IF NOT EXISTS idx_health_admission ON healthcare_records(admission_date);
        CREATE INDEX IF NOT EXISTS idx_health_patient ON healthcare_records(patient_id);
        CREATE INDEX IF NOT EXISTS idx_health_department ON healthcare_records(department);
    """
}

def get_last_processed_id(table_name: str, **context) -> int:
    """
    Get the last processed ID for incremental loading
    """
    # In production, you would query the database to get the max ID
    # For demo, we'll use XCom to track this
    ti = context['ti']
    last_id = ti.xcom_pull(key=f'{table_name}_last_id', include_prior_dates=True)
    return last_id or 0

def generate_incremental_data(table_name: str, num_rows: int, start_id: int, **context) -> str:
    """
    Generate new incremental data for the specified table
    """
    print(f"Generating {num_rows} new rows for {table_name} starting from ID {start_id}")
    
    # Create directory for incremental files
    incremental_dir = './dags/incremental_data'
    os.makedirs(incremental_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    file_path = f"{incremental_dir}/{table_name}_{timestamp}.csv"
    
    if table_name == 'employees':
        data = generate_employee_data(num_rows, start_id)
    elif table_name == 'products':
        data = generate_product_data(num_rows, start_id)
    elif table_name == 'sales_transactions':
        data = generate_sales_data(num_rows, start_id)
    elif table_name == 'support_tickets':
        data = generate_support_data(num_rows, start_id)
    elif table_name == 'web_analytics':
        data = generate_analytics_data(num_rows, start_id)
    elif table_name == 'healthcare_records':
        data = generate_healthcare_data(num_rows, start_id)
    else:
        raise ValueError(f"Unknown table: {table_name}")
    
    # Save to CSV
    data.to_csv(file_path, index=False)
    
    # Store the file path and last ID in XCom
    ti = context['ti']
    ti.xcom_push(key=f'{table_name}_file', value=file_path)
    ti.xcom_push(key=f'{table_name}_last_id', value=start_id + num_rows)
    
    return file_path

def process_incremental_file(table_name: str, **context) -> None:
    """
    Process the incremental file and generate SQL inserts
    """
    ti = context['ti']
    file_path = ti.xcom_pull(key=f'{table_name}_file')
    
    if not file_path or not os.path.exists(file_path):
        raise FileNotFoundError(f"Incremental file not found for {table_name}")
    
    # Create SQL directory
    sql_dir = './dags/sql/incremental'
    os.makedirs(sql_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    sql_file = f"{sql_dir}/{table_name}_{timestamp}.sql"
    
    # Process file in chunks
    with open(sql_file, 'w') as f:
        for chunk in pd.read_csv(file_path, chunksize=CHUNK_SIZE):
            for _, row in chunk.iterrows():
                insert_sql = generate_insert_sql(table_name, row)
                f.write(insert_sql + '\n')
    
    # Store SQL file path in XCom
    ti.xcom_push(key=f'{table_name}_sql', value=sql_file)
    
    # Clean up the CSV file
    os.remove(file_path)

def generate_insert_sql(table_name: str, row: pd.Series) -> str:
    """
    Generate INSERT SQL for a single row
    """
    columns = []
    values = []
    
    for col, val in row.items():
        columns.append(col)
        
        if pd.isna(val):
            values.append('NULL')
        elif isinstance(val, str):
            # Escape single quotes
            escaped_val = val.replace("'", "''")
            values.append(f"'{escaped_val}'")
        elif isinstance(val, bool):
            values.append('TRUE' if val else 'FALSE')
        else:
            values.append(str(val))
    
    return f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(values)}) ON CONFLICT DO NOTHING;"

# Data generation functions for each table
def generate_employee_data(num_rows: int, start_id: int) -> pd.DataFrame:
    """Generate employee data"""
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa']
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller']
    departments = ['Sales', 'Marketing', 'IT', 'HR', 'Finance', 'Operations']
    
    data = {
        'employee_id': range(start_id + 1, start_id + num_rows + 1),
        'first_name': [random.choice(first_names) for _ in range(num_rows)],
        'last_name': [random.choice(last_names) for _ in range(num_rows)],
        'age': np.random.randint(22, 67, num_rows),
        'gender': np.random.choice(['M', 'F', 'Other'], num_rows),
        'email': [f"emp{start_id + i + 1}@company.com" for i in range(num_rows)],
        'phone': [f"({random.randint(200, 999)}){random.randint(100, 999)}-{random.randint(1000, 9999)}" for _ in range(num_rows)],
        'address': [f"{random.randint(1, 9999)} {random.choice(['Main', 'Oak', 'Pine'])} St" for _ in range(num_rows)],
        'city': np.random.choice(['New York', 'Los Angeles', 'Chicago', 'Houston'], num_rows),
        'state': np.random.choice(['NY', 'CA', 'IL', 'TX'], num_rows),
        'zip_code': [str(random.randint(10000, 99999)) for _ in range(num_rows)],
        'country': np.random.choice(['USA', 'Canada', 'Mexico'], num_rows),
        'hire_date': [datetime.now() - timedelta(days=random.randint(0, 3650)) for _ in range(num_rows)],
        'department': np.random.choice(departments, num_rows),
        'job_title': np.random.choice(['Manager', 'Analyst', 'Developer', 'Specialist'], num_rows),
        'salary': np.random.randint(30000, 250000, num_rows),
        'bonus_percentage': np.random.uniform(0, 30, num_rows).round(2),
        'years_experience': np.random.randint(0, 40, num_rows),
        'education_level': np.random.choice(['High School', 'Bachelor', 'Master', 'PhD'], num_rows),
        'performance_rating': np.random.uniform(1, 5, num_rows).round(2),
        'satisfaction_score': np.random.uniform(1, 10, num_rows).round(2),
        'attendance_rate': np.random.uniform(85, 100, num_rows).round(2),
        'training_hours': np.random.randint(0, 200, num_rows),
        'certifications': np.random.randint(0, 10, num_rows),
        'is_manager': np.random.choice([True, False], num_rows),
        'team_size': np.random.randint(0, 50, num_rows),
        'remote_work_days': np.random.randint(0, 5, num_rows),
        'marital_status': np.random.choice(['Single', 'Married', 'Divorced'], num_rows),
        'dependents': np.random.randint(0, 6, num_rows),
        'emergency_contact': [f"{random.choice(first_names)} {random.choice(last_names)}" for _ in range(num_rows)]
    }
    
    return pd.DataFrame(data)

def generate_product_data(num_rows: int, start_id: int) -> pd.DataFrame:
    """Generate product data"""
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books']
    brands = ['TechCorp', 'StyleMax', 'HomeEase', 'SportPro', 'BookWorld']
    
    data = {
        'product_id': [f"PROD{str(start_id + i + 1).zfill(6)}" for i in range(num_rows)],
        'sku': [f"SKU{random.randint(100000, 999999)}" for _ in range(num_rows)],
        'product_name': [f"{random.choice(brands)} {random.choice(categories)} Item {random.randint(1, 1000)}" for _ in range(num_rows)],
        'category': np.random.choice(categories, num_rows),
        'brand': np.random.choice(brands, num_rows),
        'description': ['High-quality product' for _ in range(num_rows)],
        'unit_price': np.random.uniform(9.99, 999.99, num_rows).round(2),
        'cost_price': np.random.uniform(5.00, 500.00, num_rows).round(2),
        'quantity_in_stock': np.random.randint(0, 5000, num_rows),
        'reorder_level': np.random.randint(10, 100, num_rows),
        'reorder_quantity': np.random.randint(50, 500, num_rows),
        'warehouse_location': np.random.choice(['WH-North', 'WH-South', 'WH-East', 'WH-West'], num_rows),
        'supplier': np.random.choice(['Supplier A', 'Supplier B', 'Supplier C'], num_rows),
        'weight_kg': np.random.uniform(0.1, 50.0, num_rows).round(2),
        'dimensions_cm': [f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 100)}" for _ in range(num_rows)],
        'condition': np.random.choice(['New', 'Refurbished', 'Used'], num_rows),
        'manufactured_date': [datetime.now() - timedelta(days=random.randint(0, 730)) for _ in range(num_rows)],
        'expiry_date': [datetime.now() + timedelta(days=random.randint(365, 1095)) if random.random() < 0.3 else None for _ in range(num_rows)],
        'last_restocked': [datetime.now() - timedelta(days=random.randint(0, 30)) for _ in range(num_rows)],
        'units_sold_30days': np.random.randint(0, 500, num_rows),
        'units_sold_90days': np.random.randint(0, 1500, num_rows),
        'units_sold_365days': np.random.randint(0, 6000, num_rows),
        'rating': np.random.uniform(1.0, 5.0, num_rows).round(1),
        'review_count': np.random.randint(0, 2000, num_rows),
        'is_active': np.random.choice([True, False], num_rows, p=[0.9, 0.1]),
        'discount_percentage': np.random.uniform(0, 50, num_rows).round(1),
        'tax_rate': np.random.uniform(0, 15, num_rows).round(2),
        'shipping_class': np.random.choice(['Standard', 'Express', 'Overnight'], num_rows),
        'country_of_origin': np.random.choice(['USA', 'China', 'Germany', 'Japan'], num_rows),
        'barcode': [str(random.randint(100000000, 999999999)) for _ in range(num_rows)],
        'minimum_age': np.random.choice([0, 13, 18, 21], num_rows)
    }
    
    return pd.DataFrame(data)

def generate_sales_data(num_rows: int, start_id: int) -> pd.DataFrame:
    """Generate sales transaction data"""
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer']
    channels = ['Online', 'In-Store', 'Mobile App', 'Phone']
    
    data = {
        'transaction_id': [f"TRX{str(start_id + i + 1).zfill(8)}" for i in range(num_rows)],
        'order_date': [datetime.now() - timedelta(hours=random.randint(0, 24)) for _ in range(num_rows)],
        'customer_id': [f"CUST{str(random.randint(1, 50000)).zfill(6)}" for _ in range(num_rows)],
        'customer_name': [f"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}" for _ in range(num_rows)],
        'customer_email': [f"customer{random.randint(1, 50000)}@email.com" for _ in range(num_rows)],
        'customer_type': np.random.choice(['Regular', 'Premium', 'VIP'], num_rows),
        'product_ids': [f"PROD{random.randint(1, 100000)}" for _ in range(num_rows)],
        'product_names': [f"Product {random.randint(1, 1000)}" for _ in range(num_rows)],
        'quantities': [str(random.randint(1, 10)) for _ in range(num_rows)],
        'unit_prices': [str(round(random.uniform(9.99, 299.99), 2)) for _ in range(num_rows)],
        'subtotal': np.random.uniform(10, 5000, num_rows).round(2),
        'tax_amount': np.random.uniform(1, 500, num_rows).round(2),
        'shipping_cost': np.random.uniform(0, 50, num_rows).round(2),
        'discount_amount': np.random.uniform(0, 200, num_rows).round(2),
        'total_amount': np.random.uniform(10, 5500, num_rows).round(2),
        'payment_method': np.random.choice(payment_methods, num_rows),
        'payment_status': np.random.choice(['Completed', 'Pending', 'Failed'], num_rows, p=[0.9, 0.08, 0.02]),
        'order_status': np.random.choice(['Delivered', 'Shipped', 'Processing'], num_rows),
        'sales_channel': np.random.choice(channels, num_rows),
        'sales_rep_id': [f"REP{str(random.randint(1, 100)).zfill(3)}" for _ in range(num_rows)],
        'sales_rep_name': [f"{random.choice(['Tom', 'Amy', 'Jack'])} {random.choice(['Wilson', 'Davis', 'Moore'])}" for _ in range(num_rows)],
        'shipping_address': [f"{random.randint(1, 9999)} Main St, City, ST {random.randint(10000, 99999)}" for _ in range(num_rows)],
        'billing_address': [f"{random.randint(1, 9999)} Oak Ave, City, ST {random.randint(10000, 99999)}" for _ in range(num_rows)],
        'region': np.random.choice(['North America', 'Europe', 'Asia'], num_rows),
        'country': np.random.choice(['USA', 'Canada', 'UK', 'Germany'], num_rows),
        'currency': np.random.choice(['USD', 'EUR', 'GBP'], num_rows),
        'exchange_rate': np.random.uniform(0.8, 1.2, num_rows).round(4),
        'notes': [''] * num_rows,
        'refund_amount': np.zeros(num_rows),
        'is_gift': np.random.choice([True, False], num_rows, p=[0.1, 0.9]),
        'loyalty_points_earned': np.random.randint(0, 5000, num_rows)
    }
    
    return pd.DataFrame(data)

def generate_support_data(num_rows: int, start_id: int) -> pd.DataFrame:
    """Generate support ticket data"""
    categories = ['Technical Issue', 'Billing', 'Product Inquiry', 'Shipping', 'Returns']
    priorities = ['Low', 'Medium', 'High', 'Critical']
    statuses = ['Open', 'In Progress', 'Resolved', 'Closed']
    
    data = {
        'ticket_id': [f"TICKET{str(start_id + i + 1).zfill(7)}" for i in range(num_rows)],
        'created_date': [datetime.now() - timedelta(hours=random.randint(0, 48)) for _ in range(num_rows)],
        'customer_id': [f"CUST{str(random.randint(1, 50000)).zfill(6)}" for _ in range(num_rows)],
        'customer_name': [f"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}" for _ in range(num_rows)],
        'customer_email': [f"customer{random.randint(1, 50000)}@email.com" for _ in range(num_rows)],
        'category': np.random.choice(categories, num_rows),
        'subcategory': [f"Type {random.randint(1, 5)}" for _ in range(num_rows)],
        'priority': np.random.choice(priorities, num_rows),
        'status': np.random.choice(statuses, num_rows),
        'subject': [f"Issue #{random.randint(1000, 9999)}" for _ in range(num_rows)],
        'description': ['Customer reported an issue' for _ in range(num_rows)],
        'channel': np.random.choice(['Email', 'Phone', 'Chat', 'Web'], num_rows),
        'assigned_to': [f"AGENT{str(random.randint(1, 50)).zfill(3)}" for _ in range(num_rows)],
        'department': np.random.choice(['Level 1', 'Level 2', 'Technical'], num_rows),
        'first_response_time_hours': np.random.uniform(0.1, 24, num_rows).round(2),
        'resolution_time_hours': np.random.uniform(1, 168, num_rows).round(2),
        'number_of_interactions': np.random.randint(1, 20, num_rows),
        'satisfaction_rating': np.random.uniform(1, 5, num_rows).round(1),
        'tags': ['support,customer' for _ in range(num_rows)],
        'related_order_id': [f"TRX{str(random.randint(1, 150000)).zfill(8)}" if random.random() < 0.6 else '' for _ in range(num_rows)],
        'product_id': [f"PROD{str(random.randint(1, 100000)).zfill(6)}" if random.random() < 0.4 else '' for _ in range(num_rows)],
        'escalated': np.random.choice([True, False], num_rows, p=[0.1, 0.9]),
        'reopened': np.random.choice([True, False], num_rows, p=[0.05, 0.95]),
        'agent_notes': ['Investigation in progress' for _ in range(num_rows)],
        'resolution_notes': ['Issue resolved' if random.random() < 0.7 else '' for _ in range(num_rows)],
        'sla_breach': np.random.choice([True, False], num_rows, p=[0.15, 0.85]),
        'response_sla_hours': np.random.choice([1, 4, 24], num_rows),
        'resolution_sla_hours': np.random.choice([4, 24, 72], num_rows),
        'customer_lifetime_value': np.random.uniform(100, 10000, num_rows).round(2),
        'is_vip_customer': np.random.choice([True, False], num_rows, p=[0.2, 0.8]),
        'language': np.random.choice(['English', 'Spanish', 'French'], num_rows)
    }
    
    return pd.DataFrame(data)

def generate_analytics_data(num_rows: int, start_id: int) -> pd.DataFrame:
    """Generate web analytics data"""
    pages = ['/home', '/products', '/about', '/contact', '/checkout', '/cart']
    sources = ['Organic Search', 'Direct', 'Social Media', 'Email', 'Paid Search']
    devices = ['Desktop', 'Mobile', 'Tablet']
    
    data = {
        'session_id': [f"SESSION{str(start_id + i + 1).zfill(10)}" for i in range(num_rows)],
        'user_id': [f"USER{str(random.randint(1, 100000)).zfill(8)}" if random.random() < 0.7 else 'anonymous' for _ in range(num_rows)],
        'timestamp': [datetime.now() - timedelta(minutes=random.randint(0, 60)) for _ in range(num_rows)],
        'page_url': np.random.choice(pages, num_rows),
        'referrer_url': [random.choice(['https://google.com', 'https://facebook.com', 'direct', '']) for _ in range(num_rows)],
        'source': np.random.choice(sources, num_rows),
        'medium': np.random.choice(['organic', 'cpc', 'referral', 'email'], num_rows),
        'campaign': [f"campaign_{random.randint(1, 10)}" if random.random() < 0.3 else '' for _ in range(num_rows)],
        'device_type': np.random.choice(devices, num_rows),
        'browser': np.random.choice(['Chrome', 'Safari', 'Firefox', 'Edge'], num_rows),
        'operating_system': np.random.choice(['Windows', 'macOS', 'iOS', 'Android'], num_rows),
        'screen_resolution': np.random.choice(['1920x1080', '1366x768', '375x667'], num_rows),
        'country': np.random.choice(['USA', 'UK', 'Canada', 'Germany'], num_rows),
        'city': np.random.choice(['New York', 'London', 'Toronto', 'Berlin'], num_rows),
        'language': np.random.choice(['en-US', 'en-GB', 'de-DE', 'fr-FR'], num_rows),
        'session_duration_seconds': np.random.randint(1, 1800, num_rows),
        'pages_viewed': np.random.randint(1, 20, num_rows),
        'bounce': np.random.choice([True, False], num_rows, p=[0.3, 0.7]),
        'conversion': np.random.choice([True, False], num_rows, p=[0.1, 0.9]),
        'conversion_value': np.random.uniform(0, 500, num_rows).round(2),
        'events_triggered': np.random.randint(0, 50, num_rows),
        'click_through_rate': np.random.uniform(0, 10, num_rows).round(2),
        'scroll_depth_percentage': np.random.randint(0, 100, num_rows),
        'form_submissions': np.random.randint(0, 3, num_rows),
        'downloads': np.random.randint(0, 5, num_rows),
        'video_plays': np.random.randint(0, 10, num_rows),
        'add_to_cart': np.random.randint(0, 5, num_rows),
        'purchases': np.random.randint(0, 2, num_rows),
        'new_vs_returning': np.random.choice(['New', 'Returning'], num_rows),
        'user_agent': ['Mozilla/5.0' for _ in range(num_rows)],
        'ip_address_hash': [f"hash_{random.randint(100000, 999999)}" for _ in range(num_rows)],
        'session_quality_score': np.random.uniform(0, 100, num_rows).round(2)
    }
    
    return pd.DataFrame(data)

def generate_healthcare_data(num_rows: int, start_id: int) -> pd.DataFrame:
    """Generate healthcare record data"""
    departments = ['Emergency', 'Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics']
    diagnoses = ['Hypertension', 'Diabetes', 'Asthma', 'Arthritis', 'Depression']
    blood_types = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
    
    data = {
        'patient_id': [f"PAT{str(start_id + i + 1).zfill(8)}" for i in range(num_rows)],
        'admission_date': [datetime.now().date() - timedelta(days=random.randint(0, 30)) for _ in range(num_rows)],
        'discharge_date': [datetime.now().date() - timedelta(days=random.randint(0, 7)) for _ in range(num_rows)],
        'patient_name': [f"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}" for _ in range(num_rows)],
        'date_of_birth': [datetime.now().date() - timedelta(days=random.randint(7300, 29200)) for _ in range(num_rows)],
        'age': np.random.randint(20, 80, num_rows),
        'gender': np.random.choice(['Male', 'Female', 'Other'], num_rows),
        'blood_type': np.random.choice(blood_types, num_rows),
        'height_cm': np.random.randint(150, 200, num_rows),
        'weight_kg': np.random.uniform(45, 120, num_rows).round(1),
        'bmi': np.random.uniform(18, 35, num_rows).round(1),
        'department': np.random.choice(departments, num_rows),
        'primary_diagnosis': np.random.choice(diagnoses, num_rows),
        'secondary_diagnosis': [random.choice(diagnoses) if random.random() < 0.4 else '' for _ in range(num_rows)],
        'attending_physician': [f"Dr. {random.choice(['Smith', 'Johnson', 'Williams'])}" for _ in range(num_rows)],
        'referring_physician': [f"Dr. {random.choice(['Brown', 'Davis', 'Miller'])}" if random.random() < 0.6 else '' for _ in range(num_rows)],
        'procedures_performed': ['Blood Test; X-Ray' for _ in range(num_rows)],
        'medications_prescribed': ['Medication A; Medication B' for _ in range(num_rows)],
        'lab_results': ['Normal' for _ in range(num_rows)],
        'vital_signs': [f"BP:{random.randint(110, 140)}/{random.randint(70, 90)}" for _ in range(num_rows)],
        'insurance_provider': np.random.choice(['Blue Cross', 'Aetna', 'Cigna', 'United'], num_rows),
        'insurance_id': [f"INS{random.randint(100000000, 999999999)}" for _ in range(num_rows)],
        'copay_amount': np.random.uniform(20, 200, num_rows).round(2),
        'total_charges': np.random.uniform(500, 50000, num_rows).round(2),
        'insurance_covered': np.random.uniform(400, 40000, num_rows).round(2),
        'patient_balance': np.random.uniform(100, 10000, num_rows).round(2),
        'admission_type': np.random.choice(['Emergency', 'Elective', 'Urgent'], num_rows),
        'discharge_disposition': np.random.choice(['Home', 'Rehab', 'Skilled Nursing'], num_rows),
        'length_of_stay_days': np.random.randint(1, 14, num_rows),
        'readmission_risk': np.random.choice(['Low', 'Medium', 'High'], num_rows),
        'patient_satisfaction': np.random.uniform(1, 5, num_rows).round(1),
        'follow_up_required': np.random.choice([True, False], num_rows, p=[0.8, 0.2])
    }
    
    return pd.DataFrame(data)

# Create the main DAG
dag = DAG(
    'incremental_data_processor',
    default_args=default_args,
    description='Hourly incremental data processing for all tables',
    schedule_interval='@hourly',  # Run every hour
    catchup=False,
    max_active_runs=1,
    tags=['incremental', 'production']
)

# Create initial setup task
def initialize_tables(**context):
    """One-time setup to create all tables"""
    print("Initializing all tables...")
    # This would be run once during initial setup

initialize = PythonOperator(
    task_id='initialize_tables',
    python_callable=initialize_tables,
    dag=dag
)

# Create tasks for each table
for table_name, create_sql in TABLE_DEFINITIONS.items():
    
    # Task to create/verify table exists
    create_table = PostgresOperator(
        task_id=f'create_{table_name}_table',
        postgres_conn_id='write_to_psql',
        sql=create_sql,
        dag=dag
    )
    
    # Task to get last processed ID
    get_last_id = PythonOperator(
        task_id=f'get_{table_name}_last_id',
        python_callable=get_last_processed_id,
        op_kwargs={'table_name': table_name},
        dag=dag
    )
    
    # Task to generate new incremental data
    generate_data = PythonOperator(
        task_id=f'generate_{table_name}_data',
        python_callable=generate_incremental_data,
        op_kwargs={
            'table_name': table_name,
            'num_rows': INCREMENTAL_ROWS_PER_RUN[table_name],
            'start_id': "{{ ti.xcom_pull(task_ids='get_" + table_name + "_last_id') or 0 }}"
        },
        dag=dag
    )
    
    # Task to process the incremental file
    process_file = PythonOperator(
        task_id=f'process_{table_name}_file',
        python_callable=process_incremental_file,
        op_kwargs={'table_name': table_name},
        dag=dag
    )
    
    # Task to insert data into PostgreSQL
    insert_data = PostgresOperator(
        task_id=f'insert_{table_name}_data',
        postgres_conn_id='write_to_psql',
        sql="{{ ti.xcom_pull(task_ids='process_" + table_name + "_file', key='" + table_name + "_sql') }}",
        dag=dag
    )
    
    # Task to update statistics and indexes
    update_stats = PostgresOperator(
        task_id=f'update_{table_name}_stats',
        postgres_conn_id='write_to_psql',
        sql=f"""
            ANALYZE {table_name};
            -- Update any materialized views if needed
            -- REFRESH MATERIALIZED VIEW CONCURRENTLY {table_name}_summary;
        """,
        dag=dag
    )
    
    # Define task dependencies
    initialize >> create_table >> get_last_id >> generate_data >> process_file >> insert_data >> update_stats

# Add a final task to clean up old files
def cleanup_old_files(**context):
    """Remove SQL files older than 7 days"""
    sql_dir = './dags/sql/incremental'
    if os.path.exists(sql_dir):
        cutoff_time = datetime.now() - timedelta(days=7)
        for filename in os.listdir(sql_dir):
            file_path = os.path.join(sql_dir, filename)
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            if file_time < cutoff_time:
                os.remove(file_path)
                print(f"Removed old file: {filename}")

cleanup = PythonOperator(
    task_id='cleanup_old_files',
    python_callable=cleanup_old_files,
    trigger_rule='none_failed',
    dag=dag
)

# All table tasks should complete before cleanup
for table_name in TABLE_DEFINITIONS.keys():
    dag.get_task(f'update_{table_name}_stats') >> cleanup

if __name__ == "__main__":
    dag.cli()