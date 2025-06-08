I have this project named airflow_csv_automation_part_two.
it has this structure: airflow_csv_automation_part_two/
├── 📄 README.md                              # This file
├── 📄 LICENSE                                # MIT License
├── 📄 .gitignore                            # Git ignore patterns
├── 📄 .env                                  # Environment variables
├── 🐳 Dockerfile                            # Custom Airflow image
├── 🐳 docker-compose.yml                    # Service orchestration
├── 📄 requirements.txt                      # Python dependencies
│
├── 📂 dags/                                 # Airflow DAGs
│   ├── 📄 incremental_data_processor.py     # Hourly incremental loader
│   ├── 📄 data_monitoring_and_reporting.py  # Monitoring and reports
│   ├── 📄 simple_csv_to_postgres_dag.py     # Simple CSV loader
│   ├── 📄 csv_processor.py                  # Bulk CSV processor
│   └── 📄 simple_test_dag.py                # Test DAG
│
├── 📂 scripts/                              # Utility scripts
│   ├── 📄 generate_sample_data.py           # Generate test data
│   └── 📄 backup_postgres.sh                # Database backup
│
└── 📂 sample_files/                         # CSV input files
    └── 📄 input.csv                         # Small test file
```
the the script in the folders and files are:
1. README.md:# Apache Airflow CSV to PostgreSQL Pipeline

[![Apache Airflow](https://img.shields.io/badge/Apache%20Airflow-2.9.1-blue)](https://airflow.apache.org/)
[![Docker](https://img.shields.io/badge/Docker-Compose-2496ED)](https://www.docker.com/)
[![PostgreSQL](https://img.shields.io/badge/PostgreSQL-16.0-336791)](https://www.postgresql.org/)
[![Python](https://img.shields.io/badge/Python-3.9-3776AB)](https://www.python.org/)

A robust, production-ready data pipeline implementation using Apache Airflow, Docker, and PostgreSQL to automate reading data from CSV files and inserting it into a database.

## 📋 Table of Contents

- [Overview](#overview)
- [Features](#features)
- [Architecture](#architecture)
- [Prerequisites](#prerequisites)
- [Installation](#installation)
- [Project Structure](#project-structure)
- [Configuration](#configuration)
- [Usage](#usage)
- [DAG Implementation](#dag-implementation)
- [Troubleshooting](#troubleshooting)
- [Best Practices](#best-practices)
- [Contributing](#contributing)
- [License](#license)

## 🎯 Overview

This project demonstrates how to create a reliable data pipeline with Apache Airflow that:
- Reads data from CSV files (supporting millions of records)
- Processes and validates the data
- Automatically inserts it into a PostgreSQL database
- Provides monitoring and error handling capabilities
- Implements incremental loading for continuous data ingestion

### Key Components

- **3 Main DAGs**: Incremental processor (hourly), Monitoring (6-hourly), Simple loader (on-demand)
- **6 Data Tables**: Each with 30+ columns handling different business domains
- **875,000+ Records**: Across all tables with realistic data
- **Production Features**: Error handling, monitoring, data quality checks

## ✨ Features

- 🐳 **Dockerized Setup**: Fully containerized Airflow environment
- 📊 **Large-Scale Processing**: Handle CSV files with millions of records
- 🔄 **Incremental Loading**: Hourly data ingestion with state tracking
- 📈 **Monitoring & Reporting**: Automated data quality checks and executive dashboards
- 🔧 **Error Handling**: Built-in retry logic and failure notifications
- 🔐 **Secure Connections**: Encrypted credential storage
- 📈 **Performance Optimized**: Chunked processing, connection pooling, indexed tables

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────┐
│                     Apache Airflow                          │
├─────────────────────────────────────────────────────────────┤
│                                                             │
│  ┌─────────────────┐  ┌──────────────────┐  ┌────────────┐│
│  │ Incremental DAG │  │ Monitoring DAG   │  │ Simple DAG ││
│  │   (Hourly)      │  │  (6-hourly)      │  │ (On-demand)││
│  └────────┬────────┘  └────────┬─────────┘  └─────┬──────┘│
│           │                    │                    │       │
│  ┌────────▼────────────────────▼────────────────────▼──────┐│
│  │              PostgreSQL Database                         ││
│  ├──────────────────────────────────────────────────────────┤│
│  │ • employees          (300K+ records, 30 columns)         ││
│  │ • products           (100K+ records, 30 columns)         ││
│  │ • sales_transactions (150K+ records, 30 columns)         ││
│  │ • support_tickets    (75K+ records, 30 columns)          ││
│  │ • web_analytics      (200K+ records, 30 columns)         ││
│  │ • healthcare_records (50K+ records, 30 columns)          ││
│  └──────────────────────────────────────────────────────────┘│
└─────────────────────────────────────────────────────────────┘
```

## 📚 Prerequisites

Before you begin, ensure you have the following installed:

- **Docker Desktop** (v20.10+)
- **Docker Compose** (v2.0+)
- **Git** for version control
- **8GB RAM minimum** (16GB recommended for large datasets)
- **10GB free disk space**

## 🚀 Installation

### Quick Start

1. **Clone the repository**
```bash
git clone https://github.com/yourusername/airflow-csv-postgres.git
cd airflow-csv-postgres
```

2. **Set environment variables**
```bash
echo -e "AIRFLOW_UID=$(id -u)" >> .env
```

3. **Start the services**
```bash
docker-compose up -d
```

4. **Access Airflow UI**
- URL: http://localhost:8080
- Username: `admin`
- Password: `admin`

5. **Configure PostgreSQL Connection**
- Navigate to Admin → Connections
- Add new connection:
  - Connection Id: `write_to_psql`
  - Connection Type: `Postgres`
  - Host: `postgres`
  - Schema: `airflow`
  - Login: `airflow`
  - Password: `airflow`
  - Port: `5432`

## 📁 Project Structure

```
airflow-csv-postgres/
├── 📄 README.md                              # This file
├── 📄 LICENSE                                # MIT License
├── 📄 .gitignore                            # Git ignore patterns
├── 📄 .env                                  # Environment variables
├── 🐳 Dockerfile                            # Custom Airflow image
├── 🐳 docker-compose.yml                    # Service orchestration
├── 📄 requirements.txt                      # Python dependencies
│
├── 📂 dags/                                 # Airflow DAGs
│   ├── 📄 incremental_data_processor.py     # Hourly incremental loader
│   ├── 📄 data_monitoring_and_reporting.py  # Monitoring and reports
│   ├── 📄 simple_csv_to_postgres_dag.py     # Simple CSV loader
│   ├── 📄 csv_processor.py                  # Bulk CSV processor
│   └── 📄 simple_test_dag.py                # Test DAG
│
├── 📂 scripts/                              # Utility scripts
│   ├── 📄 generate_sample_data.py           # Generate test data
│   └── 📄 backup_postgres.sh                # Database backup
│
└── 📂 sample_files/                         # CSV input files
    └── 📄 input.csv                         # Small test file
```

## ⚙️ Configuration

### PostgreSQL Performance Tuning

The PostgreSQL instance is configured for optimal performance with large datasets:

```yaml
POSTGRESQL_SHARED_BUFFERS: "256MB"
POSTGRESQL_EFFECTIVE_CACHE_SIZE: "1GB"
POSTGRESQL_MAX_CONNECTIONS: "200"
```

### Airflow Performance Settings

```yaml
AIRFLOW__CORE__PARALLELISM: "32"
AIRFLOW__CORE__DAG_CONCURRENCY: "16"
AIRFLOW__CORE__MAX_ACTIVE_TASKS_PER_DAG: "16"
AIRFLOW__DATABASE__SQL_ALCHEMY_POOL_SIZE: "10"
```

## 🎮 Usage

### Running the Simple CSV Loader
```bash
# Trigger manually
docker exec -it <scheduler-container> airflow dags trigger simple_csv_to_postgres_dag
```

### Starting Incremental Processing
```bash
# Unpause the DAG to start hourly runs
docker exec -it <scheduler-container> airflow dags unpause incremental_data_processor
```

### Monitoring Data Quality
```bash
# Check latest report
docker exec -it <scheduler-container> cat /opt/airflow/dags/reports/executive_summary_*.json
```

## 🔧 DAG Implementation

### 1. Incremental Data Processor
- **Schedule**: Every hour
- **Purpose**: Continuously load new data
- **Features**: State tracking, chunked processing, error recovery

### 2. Data Monitoring & Reporting
- **Schedule**: Every 6 hours
- **Purpose**: Data quality checks and business metrics
- **Features**: Executive dashboards, anomaly detection, alerts

### 3. Simple CSV Processor
- **Schedule**: On-demand
- **Purpose**: One-time bulk loads
- **Features**: Basic CSV to PostgreSQL pipeline

## 🐛 Troubleshooting

### Common Issues

#### Container Won't Start
```bash
# Check logs
docker-compose logs airflow-webserver

# Ensure correct permissions
sudo chown -R $(id -u):$(id -g) logs/ dags/
```

#### DAG Not Appearing
```bash
# Check for syntax errors
docker exec -it <scheduler-container> python /opt/airflow/dags/your_dag.py

# Refresh DAGs
docker exec -it <scheduler-container> airflow dags list
```

#### Database Connection Failed
```bash
# Test connection
docker exec -it <postgres-container> psql -U airflow -d airflow
```

### Performance Issues
```sql
-- Check table sizes
SELECT 
    tablename,
    pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) AS size
FROM pg_tables 
WHERE schemaname = 'public'
ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC;
```

## 📋 Best Practices

### Data Processing
- ✅ Process files in chunks (10,000-50,000 rows)
- ✅ Use bulk inserts with ON CONFLICT handling
- ✅ Implement proper indexing strategy
- ✅ Monitor memory usage during processing

### Security
- ✅ Use Airflow Connections for credentials
- ✅ Never hardcode passwords
- ✅ Implement role-based access control
- ✅ Encrypt sensitive data

### Monitoring
- ✅ Set up email alerts for failures
- ✅ Use SLAs for critical tasks
- ✅ Regular log review
- ✅ Performance tracking

## 🤝 Contributing

Contributions are welcome! Please follow these steps:

1. Fork the repository
2. Create a feature branch (`git checkout -b feature/AmazingFeature`)
3. Commit your changes (`git commit -m 'Add AmazingFeature'`)
4. Push to the branch (`git push origin feature/AmazingFeature`)
5. Open a Pull Request

## 📄 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.

## 🙏 Acknowledgments

- Apache Airflow Community
- Docker Team  
- PostgreSQL Development Group
- All contributors

## 📞 Support

- 📧 Email: support@example.com
- 💬 Slack: [Join our channel](https://slack.example.com)
- 🐛 Issues: [GitHub Issues](https://github.com/yourusername/airflow-csv-postgres/issues)

---

Made with ❤️ by [Your Name](https://github.com/yourusername)

2. .gitignore:# Byte-compiled / optimized / DLL files
__pycache__/
*.py[cod]
*$py.class

# Airflow
logs/
airflow.db
airflow-webserver.pid
standalone_admin_password.txt

# Environment
.env
venv/
env/

# Generated files
dags/sql/incremental/*.sql
dags/incremental_data/*.csv
dags/reports/*.json
!dags/reports/archive/.gitkeep

# IDE
.vscode/
.idea/
*.swp
*.swo

# OS
.DS_Store
Thumbs.db

# Docker
.docker/

# Backup files
backups/
*.backup
*.sql.gz

3. .env: AIRFLOW_UID=50000
COMPOSE_PROJECT_NAME=airflow-csv-postgres
_PIP_ADDITIONAL_REQUIREMENTS=AIRFLOW_UID=502


4. Dockerfile: FROM apache/airflow:2.9.1-python3.9
USER root

# Install Python dependencies
COPY requirements.txt /requirements.txt
RUN pip3 install --upgrade pip
RUN pip3 install --no-cache-dir -r /requirements.txt

# Install Airflow providers
RUN pip3 install \
    apache-airflow-providers-postgres==5.10.0 \
    apache-airflow-providers-apache-spark \
    apache-airflow-providers-amazon

# Install system dependencies
RUN apt-get update && \
    apt-get install -y \
    gcc \
    python3-dev \
    openjdk-17-jdk \
    postgresql-client \
    && apt-get clean \
    && rm -rf /var/lib/apt/lists/*

# Create necessary directories with proper permissions
RUN mkdir -p /opt/airflow/dags/sql /opt/airflow/dags/incremental_data /opt/airflow/dags/reports \
    && chown -R airflow:root /opt/airflow/dags

USER airflow


5. docker-compose.yml: version: '3'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  airflow-webserver:
    image: apache/airflow:2.9.1-python3.9
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__WEBSERVER__SECRET_KEY: 'temporary_secret_key'
      _PIP_ADDITIONAL_REQUIREMENTS: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./sample_files:/opt/airflow/sample_files
    ports:
      - "8080:8080"
    command: webserver
    restart: always

  airflow-scheduler:
    image: apache/airflow:2.9.1-python3.9
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__WEBSERVER__SECRET_KEY: 'temporary_secret_key'
      _PIP_ADDITIONAL_REQUIREMENTS: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./sample_files:/opt/airflow/sample_files
    command: scheduler
    restart: always

  airflow-init:
    image: apache/airflow:2.9.1-python3.9
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      _PIP_ADDITIONAL_REQUIREMENTS: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres'
    command: |
      bash -c "
        airflow db init &&
        airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
        airflow connections add 'write_to_psql' --conn-type 'postgres' --conn-host 'postgres' --conn-schema 'airflow' --conn-login 'airflow' --conn-password 'airflow' --conn-port 5432 || true
      "

  pgadmin:
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@pgadmin.com
      PGADMIN_DEFAULT_PASSWORD: admin
    ports:
      - "5050:80"
    restart: always
    depends_on:
      - postgres

volumes:
  postgres-db-volume:


6. requirements.txt: # Data Processing
pandas==2.0.3
numpy==1.24.3
psycopg2-binary==2.9.9

# Performance & Memory Management
pyarrow==14.0.1
fastparquet==2023.10.1

# Data Quality & Validation (optional but recommended)
great-expectations==0.18.8
sqlalchemy==1.4.51

# Monitoring & Reporting
tabulate==0.9.0
matplotlib==3.7.2
seaborn==0.12.2

# Utilities
python-dateutil==2.8.2
pytz==2023.3

7. setup_script.sh: #!/bin/bash

# Switch to Proper Airflow Setup
echo "🔄 Switching to Proper Airflow Setup"
echo "===================================="

# 1. Stop current setup
echo "1. Stopping current setup..."
docker-compose down

# 2. Create new docker-compose.yml
echo "2. Creating proper docker-compose.yml..."
cp docker-compose.yml docker-compose.yml.backup
cat > docker-compose.yml << 'EOF'
version: '3'

services:
  postgres:
    image: postgres:13
    environment:
      POSTGRES_USER: airflow
      POSTGRES_PASSWORD: airflow
      POSTGRES_DB: airflow
    volumes:
      - postgres-db-volume:/var/lib/postgresql/data
    healthcheck:
      test: ["CMD", "pg_isready", "-U", "airflow"]
      interval: 5s
      retries: 5
    restart: always

  airflow-webserver:
    image: apache/airflow:2.9.1-python3.9
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__WEBSERVER__SECRET_KEY: 'temporary_secret_key'
      _PIP_ADDITIONAL_REQUIREMENTS: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./sample_files:/opt/airflow/sample_files
    ports:
      - "8080:8080"
    command: webserver
    restart: always

  airflow-scheduler:
    image: apache/airflow:2.9.1-python3.9
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      AIRFLOW__CORE__LOAD_EXAMPLES: 'false'
      AIRFLOW__WEBSERVER__SECRET_KEY: 'temporary_secret_key'
      _PIP_ADDITIONAL_REQUIREMENTS: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres'
    volumes:
      - ./dags:/opt/airflow/dags
      - ./logs:/opt/airflow/logs
      - ./plugins:/opt/airflow/plugins
      - ./sample_files:/opt/airflow/sample_files
    command: scheduler
    restart: always

  airflow-init:
    image: apache/airflow:2.9.1-python3.9
    depends_on:
      postgres:
        condition: service_healthy
    environment:
      AIRFLOW__CORE__EXECUTOR: LocalExecutor
      AIRFLOW__DATABASE__SQL_ALCHEMY_CONN: postgresql+psycopg2://airflow:airflow@postgres/airflow
      _PIP_ADDITIONAL_REQUIREMENTS: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres'
    command: |
      bash -c "
        airflow db init &&
        airflow users create --username admin --password admin --firstname Admin --lastname User --role Admin --email admin@example.com &&
        airflow connections add 'write_to_psql' --conn-type 'postgres' --conn-host 'postgres' --conn-schema 'airflow' --conn-login 'airflow' --conn-password 'airflow' --conn-port 5432 || true
      "

  pgadmin:
    image: dpage/pgadmin4:latest
    environment:
      PGADMIN_DEFAULT_EMAIL: admin@pgadmin.com
      PGADMIN_DEFAULT_PASSWORD: admin
    ports:
      - "5050:80"
    restart: always
    depends_on:
      - postgres

volumes:
  postgres-db-volume:
EOF

# 3. Start PostgreSQL
echo "3. Starting PostgreSQL..."
docker-compose up -d postgres

# 4. Wait for PostgreSQL
echo "4. Waiting for PostgreSQL..."
sleep 10

# 5. Initialize Airflow
echo "5. Initializing Airflow..."
docker-compose run --rm airflow-init

# 6. Start all services
echo "6. Starting all services..."
docker-compose up -d

# 7. Wait for services
echo "7. Waiting for services to start..."
sleep 20

# 8. Check status
echo "8. Service status:"
docker-compose ps

echo ""
echo "✅ Setup complete!"
echo "=================="
echo "Airflow UI: http://localhost:8080 (admin/admin)"
echo "pgAdmin: http://localhost:5050 (admin@pgadmin.com/admin)"

8. fix_all_issues.sh: #!/bin/bash

# Fix all issues - permissions and data generation
echo "🔧 Fixing permission and data generation issues..."

# 1. Fix SQL directory permissions in containers
echo "1. Fixing SQL directory permissions..."
docker-compose exec -T airflow-scheduler bash -c "
    mkdir -p /opt/airflow/dags/sql
    chmod 777 /opt/airflow/dags/sql
    chown airflow:root /opt/airflow/dags/sql
"

docker-compose exec -T airflow-webserver bash -c "
    mkdir -p /opt/airflow/dags/sql
    chmod 777 /opt/airflow/dags/sql
    chown airflow:root /opt/airflow/dags/sql
"

# 2. Fix the data generation script
echo "2. Fixing data generation script..."
cat > /tmp/fix_generate_sales.py << 'EOF'
# Fix for generate_sales_data function
def generate_sales_data(num_rows, start_id):
    """Generate sales transaction data"""
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer']
    channels = ['Online', 'In-Store', 'Mobile App', 'Phone']
    statuses = ['Delivered', 'Shipped', 'Processing', 'Cancelled', 'Refunded']
    
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
        'order_status': np.random.choice(statuses, num_rows),
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
    
    # Fix the refund_amount after order_status is created
    df = pd.DataFrame(data)
    df.loc[df['order_status'] == 'Refunded', 'refund_amount'] = df.loc[df['order_status'] == 'Refunded', 'total_amount']
    
    return df
EOF

# 3. Complete data generation with fixed script
echo "3. Completing data generation..."
docker-compose exec -T airflow-scheduler bash -c "
    cd /opt/airflow/sample_files
    
    # Create a Python script to generate remaining files
    cat > complete_generation.py << 'PYEOF'
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed
np.random.seed(42)
random.seed(42)

print('Generating remaining CSV files...')

# Generate sales_transactions.csv (150,000 rows)
print('Generating sales_transactions.csv...')
payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer']
channels = ['Online', 'In-Store', 'Mobile App', 'Phone']
statuses = ['Delivered', 'Shipped', 'Processing', 'Cancelled', 'Refunded']
num_rows = 150000

data = {
    'transaction_id': [f\"TRX{str(i + 1).zfill(8)}\" for i in range(num_rows)],
    'order_date': [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_rows)],
    'customer_id': [f\"CUST{str(random.randint(1, 50000)).zfill(6)}\" for _ in range(num_rows)],
    'customer_name': [f\"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}\" for _ in range(num_rows)],
    'customer_email': [f\"customer{random.randint(1, 50000)}@email.com\" for _ in range(num_rows)],
    'customer_type': np.random.choice(['Regular', 'Premium', 'VIP'], num_rows),
    'product_ids': [';'.join([f\"PROD{random.randint(1, 100000)}\" for _ in range(random.randint(1, 3))]) for _ in range(num_rows)],
    'product_names': [';'.join([f\"Product {random.randint(1, 1000)}\" for _ in range(random.randint(1, 3))]) for _ in range(num_rows)],
    'quantities': [';'.join([str(random.randint(1, 10)) for _ in range(random.randint(1, 3))]) for _ in range(num_rows)],
    'unit_prices': [';'.join([str(round(random.uniform(9.99, 299.99), 2)) for _ in range(random.randint(1, 3))]) for _ in range(num_rows)],
    'subtotal': np.random.uniform(10, 5000, num_rows).round(2),
    'tax_amount': np.random.uniform(1, 500, num_rows).round(2),
    'shipping_cost': np.random.uniform(0, 50, num_rows).round(2),
    'discount_amount': np.random.uniform(0, 200, num_rows).round(2),
    'total_amount': np.random.uniform(10, 5500, num_rows).round(2),
    'payment_method': np.random.choice(payment_methods, num_rows),
    'payment_status': np.random.choice(['Completed', 'Pending', 'Failed'], num_rows, p=[0.9, 0.08, 0.02]),
    'order_status': np.random.choice(statuses, num_rows),
    'sales_channel': np.random.choice(channels, num_rows),
    'sales_rep_id': [f\"REP{str(random.randint(1, 100)).zfill(3)}\" for _ in range(num_rows)],
    'sales_rep_name': [f\"{random.choice(['Tom', 'Amy', 'Jack'])} {random.choice(['Wilson', 'Davis', 'Moore'])}\" for _ in range(num_rows)],
    'shipping_address': [f\"{random.randint(1, 9999)} Main St, City, ST {random.randint(10000, 99999)}\" for _ in range(num_rows)],
    'billing_address': [f\"{random.randint(1, 9999)} Oak Ave, City, ST {random.randint(10000, 99999)}\" for _ in range(num_rows)],
    'region': np.random.choice(['North America', 'Europe', 'Asia'], num_rows),
    'country': np.random.choice(['USA', 'Canada', 'UK', 'Germany'], num_rows),
    'currency': np.random.choice(['USD', 'EUR', 'GBP'], num_rows),
    'exchange_rate': np.random.uniform(0.8, 1.2, num_rows).round(4),
    'notes': [''] * num_rows,
    'refund_amount': np.zeros(num_rows),
    'is_gift': np.random.choice([False, True], num_rows, p=[0.9, 0.1]),
    'loyalty_points_earned': np.random.randint(0, 5000, num_rows)
}

df = pd.DataFrame(data)
df.loc[df['order_status'] == 'Refunded', 'refund_amount'] = df.loc[df['order_status'] == 'Refunded', 'total_amount']
df.to_csv('sales_transactions.csv', index=False)
print(f'✅ Created sales_transactions.csv with {len(df)} rows')

# Generate customer_support_tickets.csv (75,000 rows)
print('Generating customer_support_tickets.csv...')
categories = ['Technical Issue', 'Billing', 'Product Inquiry', 'Shipping', 'Returns']
priorities = ['Low', 'Medium', 'High', 'Critical']
statuses = ['Open', 'In Progress', 'Resolved', 'Closed']
num_rows = 75000

data = {
    'ticket_id': [f\"TICKET{str(i + 1).zfill(7)}\" for i in range(num_rows)],
    'created_date': [(datetime.now() - timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_rows)],
    'customer_id': [f\"CUST{str(random.randint(1, 50000)).zfill(6)}\" for _ in range(num_rows)],
    'customer_name': [f\"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}\" for _ in range(num_rows)],
    'customer_email': [f\"customer{random.randint(1, 50000)}@email.com\" for _ in range(num_rows)],
    'category': np.random.choice(categories, num_rows),
    'subcategory': [f\"Type {random.randint(1, 5)}\" for _ in range(num_rows)],
    'priority': np.random.choice(priorities, num_rows),
    'status': np.random.choice(statuses, num_rows),
    'subject': [f\"Issue #{random.randint(1000, 9999)}\" for _ in range(num_rows)],
    'description': ['Customer reported an issue' for _ in range(num_rows)],
    'channel': np.random.choice(['Email', 'Phone', 'Chat', 'Web'], num_rows),
    'assigned_to': [f\"AGENT{str(random.randint(1, 50)).zfill(3)}\" for _ in range(num_rows)],
    'department': np.random.choice(['Level 1', 'Level 2', 'Technical'], num_rows),
    'first_response_time_hours': np.random.uniform(0.1, 24, num_rows).round(2),
    'resolution_time_hours': np.random.uniform(1, 168, num_rows).round(2),
    'number_of_interactions': np.random.randint(1, 20, num_rows),
    'satisfaction_rating': np.random.uniform(1, 5, num_rows).round(1),
    'tags': ['support,customer' for _ in range(num_rows)],
    'related_order_id': [f\"TRX{str(random.randint(1, 150000)).zfill(8)}\" if random.random() < 0.6 else '' for _ in range(num_rows)],
    'product_id': [f\"PROD{str(random.randint(1, 100000)).zfill(6)}\" if random.random() < 0.4 else '' for _ in range(num_rows)],
    'escalated': np.random.choice([False, True], num_rows, p=[0.9, 0.1]),
    'reopened': np.random.choice([False, True], num_rows, p=[0.95, 0.05]),
    'agent_notes': ['Investigation in progress' for _ in range(num_rows)],
    'resolution_notes': ['Issue resolved' if random.random() < 0.7 else '' for _ in range(num_rows)],
    'sla_breach': np.random.choice([False, True], num_rows, p=[0.85, 0.15]),
    'response_sla_hours': np.random.choice([1, 4, 24], num_rows),
    'resolution_sla_hours': np.random.choice([4, 24, 72], num_rows),
    'customer_lifetime_value': np.random.uniform(100, 10000, num_rows).round(2),
    'is_vip_customer': np.random.choice([False, True], num_rows, p=[0.8, 0.2]),
    'language': np.random.choice(['English', 'Spanish', 'French'], num_rows)
}

df = pd.DataFrame(data)
df.to_csv('customer_support_tickets.csv', index=False)
print(f'✅ Created customer_support_tickets.csv with {len(df)} rows')

# Generate website_analytics.csv (200,000 rows)
print('Generating website_analytics.csv...')
pages = ['/home', '/products', '/about', '/contact', '/checkout', '/cart']
sources = ['Organic Search', 'Direct', 'Social Media', 'Email', 'Paid Search']
devices = ['Desktop', 'Mobile', 'Tablet']
num_rows = 200000

data = {
    'session_id': [f\"SESSION{str(i + 1).zfill(10)}\" for i in range(num_rows)],
    'user_id': [f\"USER{str(random.randint(1, 100000)).zfill(8)}\" if random.random() < 0.7 else 'anonymous' for _ in range(num_rows)],
    'timestamp': [(datetime.now() - timedelta(hours=random.randint(0, 720))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_rows)],
    'page_url': np.random.choice(pages, num_rows),
    'referrer_url': [random.choice(['https://google.com', 'https://facebook.com', 'direct', '']) for _ in range(num_rows)],
    'source': np.random.choice(sources, num_rows),
    'medium': np.random.choice(['organic', 'cpc', 'referral', 'email'], num_rows),
    'campaign': [f\"campaign_{random.randint(1, 10)}\" if random.random() < 0.3 else '' for _ in range(num_rows)],
    'device_type': np.random.choice(devices, num_rows),
    'browser': np.random.choice(['Chrome', 'Safari', 'Firefox', 'Edge'], num_rows),
    'operating_system': np.random.choice(['Windows', 'macOS', 'iOS', 'Android'], num_rows),
    'screen_resolution': np.random.choice(['1920x1080', '1366x768', '375x667'], num_rows),
    'country': np.random.choice(['USA', 'UK', 'Canada', 'Germany'], num_rows),
    'city': np.random.choice(['New York', 'London', 'Toronto', 'Berlin'], num_rows),
    'language': np.random.choice(['en-US', 'en-GB', 'de-DE', 'fr-FR'], num_rows),
    'session_duration_seconds': np.random.randint(1, 1800, num_rows),
    'pages_viewed': np.random.randint(1, 20, num_rows),
    'bounce': np.random.choice([False, True], num_rows, p=[0.7, 0.3]),
    'conversion': np.random.choice([False, True], num_rows, p=[0.9, 0.1]),
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
    'ip_address_hash': [f\"hash_{random.randint(100000, 999999)}\" for _ in range(num_rows)],
    'session_quality_score': np.random.uniform(0, 100, num_rows).round(2)
}

df = pd.DataFrame(data)
df.to_csv('website_analytics.csv', index=False)
print(f'✅ Created website_analytics.csv with {len(df)} rows')

# Generate healthcare_records.csv (50,000 rows)
print('Generating healthcare_records.csv...')
departments = ['Emergency', 'Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics']
diagnoses = ['Hypertension', 'Diabetes', 'Asthma', 'Arthritis', 'Depression']
blood_types = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
num_rows = 50000

data = {
    'patient_id': [f\"PAT{str(i + 1).zfill(8)}\" for i in range(num_rows)],
    'admission_date': [(datetime.now().date() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') for _ in range(num_rows)],
    'discharge_date': [(datetime.now().date() - timedelta(days=random.randint(0, 7))).strftime('%Y-%m-%d') for _ in range(num_rows)],
    'patient_name': [f\"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}\" for _ in range(num_rows)],
    'date_of_birth': [(datetime.now().date() - timedelta(days=random.randint(7300, 29200))).strftime('%Y-%m-%d') for _ in range(num_rows)],
    'age': np.random.randint(20, 80, num_rows),
    'gender': np.random.choice(['Male', 'Female', 'Other'], num_rows),
    'blood_type': np.random.choice(blood_types, num_rows),
    'height_cm': np.random.randint(150, 200, num_rows),
    'weight_kg': np.random.uniform(45, 120, num_rows).round(1),
    'bmi': np.random.uniform(18, 35, num_rows).round(1),
    'department': np.random.choice(departments, num_rows),
    'primary_diagnosis': np.random.choice(diagnoses, num_rows),
    'secondary_diagnosis': [random.choice(diagnoses) if random.random() < 0.4 else '' for _ in range(num_rows)],
    'attending_physician': [f\"Dr. {random.choice(['Smith', 'Johnson', 'Williams'])}\" for _ in range(num_rows)],
    'referring_physician': [f\"Dr. {random.choice(['Brown', 'Davis', 'Miller'])}\" if random.random() < 0.6 else '' for _ in range(num_rows)],
    'procedures_performed': ['Blood Test; X-Ray' for _ in range(num_rows)],
    'medications_prescribed': ['Medication A; Medication B' for _ in range(num_rows)],
    'lab_results': ['Normal' for _ in range(num_rows)],
    'vital_signs': [f\"BP:{random.randint(110, 140)}/{random.randint(70, 90)}\" for _ in range(num_rows)],
    'insurance_provider': np.random.choice(['Blue Cross', 'Aetna', 'Cigna', 'United'], num_rows),
    'insurance_id': [f\"INS{random.randint(100000000, 999999999)}\" for _ in range(num_rows)],
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

df = pd.DataFrame(data)
df.to_csv('healthcare_records.csv', index=False)
print(f'✅ Created healthcare_records.csv with {len(df)} rows')

print('\\nAll CSV files generated successfully!')
print('Total files: 6')
print('Files location: /opt/airflow/sample_files/')
PYEOF

    # Run the generation script
    python complete_generation.py
    
    # List all files
    echo ''
    echo 'All generated files:'
    ls -la *.csv
"

# 4. Copy files to webserver too
echo "4. Syncing files to webserver..."
docker-compose exec -T airflow-scheduler bash -c "
    cp -r /opt/airflow/sample_files/* /tmp/
"
docker cp airflow-csv-postgres-airflow-scheduler-1:/tmp/sample_files .
docker cp sample_files airflow-csv-postgres-airflow-webserver-1:/opt/airflow/

# 5. Test simple_csv_to_postgres_dag again
echo ""
echo "5. Testing simple_csv_to_postgres_dag with fixed permissions..."
docker-compose exec airflow-scheduler airflow dags test simple_csv_to_postgres_dag 2024-01-01

echo ""
echo "✅ All issues fixed!"
echo "- SQL directory permissions fixed"
echo "- All 6 CSV files generated"
echo "- Ready to run DAGs!"

9. generate-data-docker.sh: #!/bin/bash

# Generate sample data inside Docker container
echo "📊 Generating sample data inside Docker container..."

# Copy the script to the scheduler container
docker cp scripts/generate_sample_data.py airflow-csv-postgres-airflow-scheduler-1:/tmp/

# Execute the script inside the container
docker-compose exec airflow-scheduler bash -c "
    cd /tmp
    
    # The script expects to create sample_files in current directory
    python generate_sample_data.py
    
    # Copy generated files to the airflow directory
    cp -r sample_files /opt/airflow/
    
    # List the generated files
    echo ''
    echo 'Generated files:'
    ls -la /opt/airflow/sample_files/
    
    # Show file sizes
    echo ''
    echo 'File sizes:'
    du -h /opt/airflow/sample_files/*
"

# Also copy to the webserver container
docker-compose exec airflow-scheduler bash -c "
    cp -r /opt/airflow/sample_files /tmp/
"
docker cp airflow-csv-postgres-airflow-scheduler-1:/tmp/sample_files .
docker cp sample_files airflow-csv-postgres-airflow-webserver-1:/opt/airflow/

echo ""
echo "✅ Sample data generated successfully!"
echo "Files are now available in both containers at /opt/airflow/sample_files/"

# Test the simple CSV DAG now that we have data
echo ""
echo "Testing simple_csv_to_postgres_dag with sample data..."
docker-compose exec airflow-scheduler airflow dags test simple_csv_to_postgres_dag 2024-01-01


10. setup-directories-script.sh: #!/bin/bash

# Setup directories and fix permissions in Airflow containers
echo "📁 Setting up directories in Airflow containers..."

# Create directories in scheduler
echo "Creating directories in scheduler..."
docker-compose exec -T airflow-scheduler bash -c "
    mkdir -p /opt/airflow/dags/sql/incremental
    mkdir -p /opt/airflow/dags/incremental_data
    mkdir -p /opt/airflow/dags/reports/archive
    chmod -R 777 /opt/airflow/dags/sql
    chmod -R 777 /opt/airflow/dags/incremental_data
    chmod -R 777 /opt/airflow/dags/reports
"

# Create directories in webserver
echo "Creating directories in webserver..."
docker-compose exec -T airflow-webserver bash -c "
    mkdir -p /opt/airflow/dags/sql/incremental
    mkdir -p /opt/airflow/dags/incremental_data
    mkdir -p /opt/airflow/dags/reports/archive
    chmod -R 777 /opt/airflow/dags/sql
    chmod -R 777 /opt/airflow/dags/incremental_data
    chmod -R 777 /opt/airflow/dags/reports
"

# Copy sample files if they exist
if [ -d "sample_files" ]; then
    echo "Copying sample files..."
    docker cp sample_files airflow-csv-postgres-airflow-scheduler-1:/opt/airflow/
    docker cp sample_files airflow-csv-postgres-airflow-webserver-1:/opt/airflow/
fi

echo "✅ Directories created successfully!"

# Test a simple DAG
echo ""
echo "Testing simple_test_dag..."
docker-compose exec airflow-scheduler airflow dags test simple_test_dag 2024-01-01

echo ""
echo "🎉 Setup complete! Your DAGs are ready to run."
echo ""
echo "Available DAGs:"
docker-compose exec airflow-scheduler airflow dags list


The folders are:
1. dags. this also has the following files:
1. incremental_data_processor.py: from airflow import DAG
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

2. data_monitoring_and_reporting.py: from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook
from datetime import datetime, timedelta
import pandas as pd
import json
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    'email_on_failure': True,
    'email': ['analytics@company.com'],
}

# SQL queries for monitoring and reporting
MONITORING_QUERIES = {
    'employee_metrics': """
        WITH employee_stats AS (
            SELECT 
                COUNT(*) as total_employees,
                COUNT(CASE WHEN is_active = true THEN 1 END) as active_employees,
                COUNT(CASE WHEN is_manager = true THEN 1 END) as total_managers,
                AVG(salary) as avg_salary,
                AVG(performance_rating) as avg_performance,
                AVG(satisfaction_score) as avg_satisfaction,
                COUNT(DISTINCT department) as num_departments
            FROM employees
            WHERE created_at >= NOW() - INTERVAL '24 hours'
        ),
        department_breakdown AS (
            SELECT 
                department,
                COUNT(*) as dept_count,
                AVG(salary) as dept_avg_salary
            FROM employees
            WHERE is_active = true
            GROUP BY department
            ORDER BY dept_count DESC
            LIMIT 5
        )
        SELECT 
            'employee_summary' as metric_type,
            json_build_object(
                'total_stats', (SELECT row_to_json(employee_stats) FROM employee_stats),
                'top_departments', (SELECT json_agg(row_to_json(department_breakdown)) FROM department_breakdown)
            ) as metrics
    """,
    
    'sales_metrics': """
        WITH daily_sales AS (
            SELECT 
                DATE(order_date) as sale_date,
                COUNT(*) as transaction_count,
                SUM(total_amount) as daily_revenue,
                AVG(total_amount) as avg_transaction_value,
                COUNT(DISTINCT customer_id) as unique_customers
            FROM sales_transactions
            WHERE order_date >= NOW() - INTERVAL '7 days'
            GROUP BY DATE(order_date)
            ORDER BY sale_date DESC
        ),
        channel_performance AS (
            SELECT 
                sales_channel,
                COUNT(*) as channel_transactions,
                SUM(total_amount) as channel_revenue,
                AVG(total_amount) as avg_order_value
            FROM sales_transactions
            WHERE order_date >= NOW() - INTERVAL '24 hours'
            GROUP BY sales_channel
        ),
        payment_status AS (
            SELECT 
                payment_status,
                COUNT(*) as status_count,
                SUM(total_amount) as status_amount
            FROM sales_transactions
            WHERE order_date >= NOW() - INTERVAL '24 hours'
            GROUP BY payment_status
        )
        SELECT 
            'sales_summary' as metric_type,
            json_build_object(
                'daily_trends', (SELECT json_agg(row_to_json(daily_sales)) FROM daily_sales),
                'channel_performance', (SELECT json_agg(row_to_json(channel_performance)) FROM channel_performance),
                'payment_status', (SELECT json_agg(row_to_json(payment_status)) FROM payment_status)
            ) as metrics
    """,
    
    'product_metrics': """
        WITH inventory_alerts AS (
            SELECT 
                COUNT(CASE WHEN quantity_in_stock <= reorder_level THEN 1 END) as low_stock_items,
                COUNT(CASE WHEN quantity_in_stock = 0 THEN 1 END) as out_of_stock_items,
                COUNT(*) as total_active_products,
                SUM(quantity_in_stock * unit_price) as total_inventory_value
            FROM products
            WHERE is_active = true
        ),
        top_performers AS (
            SELECT 
                product_id,
                product_name,
                category,
                units_sold_30days,
                rating,
                review_count
            FROM products
            WHERE is_active = true
            ORDER BY units_sold_30days DESC
            LIMIT 10
        ),
        category_performance AS (
            SELECT 
                category,
                COUNT(*) as products_in_category,
                AVG(rating) as avg_category_rating,
                SUM(units_sold_30days) as category_sales_30d
            FROM products
            WHERE is_active = true
            GROUP BY category
            ORDER BY category_sales_30d DESC
        )
        SELECT 
            'product_summary' as metric_type,
            json_build_object(
                'inventory_status', (SELECT row_to_json(inventory_alerts) FROM inventory_alerts),
                'top_products', (SELECT json_agg(row_to_json(top_performers)) FROM top_performers),
                'category_performance', (SELECT json_agg(row_to_json(category_performance)) FROM category_performance)
            ) as metrics
    """,
    
    'support_metrics': """
        WITH ticket_stats AS (
            SELECT 
                COUNT(*) as total_tickets,
                COUNT(CASE WHEN status IN ('Open', 'In Progress') THEN 1 END) as open_tickets,
                COUNT(CASE WHEN status = 'Resolved' THEN 1 END) as resolved_tickets,
                AVG(CASE WHEN status = 'Resolved' THEN resolution_time_hours END) as avg_resolution_time,
                COUNT(CASE WHEN sla_breach = true THEN 1 END) as sla_breaches,
                AVG(CASE WHEN satisfaction_rating > 0 THEN satisfaction_rating END) as avg_satisfaction
            FROM support_tickets
            WHERE created_date >= NOW() - INTERVAL '24 hours'
        ),
        priority_breakdown AS (
            SELECT 
                priority,
                COUNT(*) as priority_count,
                AVG(first_response_time_hours) as avg_response_time
            FROM support_tickets
            WHERE created_date >= NOW() - INTERVAL '24 hours'
            GROUP BY priority
            ORDER BY 
                CASE priority 
                    WHEN 'Critical' THEN 1
                    WHEN 'High' THEN 2
                    WHEN 'Medium' THEN 3
                    WHEN 'Low' THEN 4
                END
        ),
        category_issues AS (
            SELECT 
                category,
                COUNT(*) as issue_count,
                COUNT(CASE WHEN escalated = true THEN 1 END) as escalated_count
            FROM support_tickets
            WHERE created_date >= NOW() - INTERVAL '7 days'
            GROUP BY category
            ORDER BY issue_count DESC
        )
        SELECT 
            'support_summary' as metric_type,
            json_build_object(
                'ticket_stats', (SELECT row_to_json(ticket_stats) FROM ticket_stats),
                'priority_breakdown', (SELECT json_agg(row_to_json(priority_breakdown)) FROM priority_breakdown),
                'category_issues', (SELECT json_agg(row_to_json(category_issues)) FROM category_issues)
            ) as metrics
    """,
    
    'analytics_metrics': """
        WITH session_stats AS (
            SELECT 
                COUNT(*) as total_sessions,
                COUNT(DISTINCT user_id) as unique_users,
                AVG(session_duration_seconds) as avg_session_duration,
                AVG(pages_viewed) as avg_pages_per_session,
                COUNT(CASE WHEN bounce = true THEN 1 END) * 100.0 / COUNT(*) as bounce_rate,
                COUNT(CASE WHEN conversion = true THEN 1 END) * 100.0 / COUNT(*) as conversion_rate,
                SUM(conversion_value) as total_conversion_value
            FROM web_analytics
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
        ),
        traffic_sources AS (
            SELECT 
                source,
                COUNT(*) as source_sessions,
                COUNT(CASE WHEN conversion = true THEN 1 END) as source_conversions,
                AVG(session_duration_seconds) as avg_duration
            FROM web_analytics
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY source
            ORDER BY source_sessions DESC
        ),
        device_breakdown AS (
            SELECT 
                device_type,
                COUNT(*) as device_sessions,
                AVG(pages_viewed) as avg_pages,
                COUNT(CASE WHEN bounce = true THEN 1 END) * 100.0 / COUNT(*) as device_bounce_rate
            FROM web_analytics
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY device_type
        ),
        top_pages AS (
            SELECT 
                page_url,
                COUNT(*) as page_views,
                AVG(scroll_depth_percentage) as avg_scroll_depth
            FROM web_analytics
            WHERE timestamp >= NOW() - INTERVAL '24 hours'
            GROUP BY page_url
            ORDER BY page_views DESC
            LIMIT 10
        )
        SELECT 
            'analytics_summary' as metric_type,
            json_build_object(
                'session_stats', (SELECT row_to_json(session_stats) FROM session_stats),
                'traffic_sources', (SELECT json_agg(row_to_json(traffic_sources)) FROM traffic_sources),
                'device_breakdown', (SELECT json_agg(row_to_json(device_breakdown)) FROM device_breakdown),
                'top_pages', (SELECT json_agg(row_to_json(top_pages)) FROM top_pages)
            ) as metrics
    """,
    
    'healthcare_metrics': """
        WITH patient_stats AS (
            SELECT 
                COUNT(*) as total_admissions,
                AVG(length_of_stay_days) as avg_length_of_stay,
                COUNT(DISTINCT department) as active_departments,
                AVG(patient_satisfaction) as avg_patient_satisfaction,
                COUNT(CASE WHEN readmission_risk = 'High' THEN 1 END) as high_risk_patients
            FROM healthcare_records
            WHERE admission_date >= CURRENT_DATE - INTERVAL '30 days'
        ),
        department_utilization AS (
            SELECT 
                department,
                COUNT(*) as dept_admissions,
                AVG(length_of_stay_days) as dept_avg_stay,
                AVG(total_charges) as dept_avg_charges
            FROM healthcare_records
            WHERE admission_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY department
            ORDER BY dept_admissions DESC
        ),
        diagnosis_frequency AS (
            SELECT 
                primary_diagnosis,
                COUNT(*) as diagnosis_count,
                AVG(total_charges) as avg_treatment_cost,
                AVG(length_of_stay_days) as avg_stay_days
            FROM healthcare_records
            WHERE admission_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY primary_diagnosis
            ORDER BY diagnosis_count DESC
            LIMIT 10
        ),
        insurance_analysis AS (
            SELECT 
                insurance_provider,
                COUNT(*) as patient_count,
                AVG(insurance_covered / NULLIF(total_charges, 0) * 100) as avg_coverage_percentage,
                SUM(patient_balance) as total_patient_balance
            FROM healthcare_records
            WHERE admission_date >= CURRENT_DATE - INTERVAL '30 days'
            GROUP BY insurance_provider
            ORDER BY patient_count DESC
        )
        SELECT 
            'healthcare_summary' as metric_type,
            json_build_object(
                'patient_stats', (SELECT row_to_json(patient_stats) FROM patient_stats),
                'department_utilization', (SELECT json_agg(row_to_json(department_utilization)) FROM department_utilization),
                'diagnosis_frequency', (SELECT json_agg(row_to_json(diagnosis_frequency)) FROM diagnosis_frequency),
                'insurance_analysis', (SELECT json_agg(row_to_json(insurance_analysis)) FROM insurance_analysis)
            ) as metrics
    """
}

# Data quality check queries
DATA_QUALITY_QUERIES = {
    'employees_quality': """
        SELECT 
            'employees' as table_name,
            COUNT(*) as total_records,
            COUNT(*) - COUNT(email) as null_emails,
            COUNT(*) - COUNT(DISTINCT email) as duplicate_emails,
            COUNT(CASE WHEN age < 18 OR age > 100 THEN 1 END) as invalid_ages,
            COUNT(CASE WHEN salary < 0 THEN 1 END) as invalid_salaries,
            MAX(created_at) as last_record_created
        FROM employees
    """,
    
    'products_quality': """
        SELECT 
            'products' as table_name,
            COUNT(*) as total_records,
            COUNT(*) - COUNT(DISTINCT sku) as duplicate_skus,
            COUNT(CASE WHEN unit_price <= cost_price THEN 1 END) as negative_margin_products,
            COUNT(CASE WHEN quantity_in_stock < 0 THEN 1 END) as negative_stock,
            COUNT(CASE WHEN rating < 1 OR rating > 5 THEN 1 END) as invalid_ratings,
            MAX(created_at) as last_record_created
        FROM products
    """,
    
    'sales_quality': """
        SELECT 
            'sales_transactions' as table_name,
            COUNT(*) as total_records,
            COUNT(CASE WHEN total_amount < 0 THEN 1 END) as negative_amounts,
            COUNT(CASE WHEN order_date > NOW() THEN 1 END) as future_dates,
            COUNT(*) - COUNT(customer_email) as missing_emails,
            COUNT(CASE WHEN payment_status = 'Failed' THEN 1 END) as failed_payments,
            MAX(created_at) as last_record_created
        FROM sales_transactions
    """
}

def execute_monitoring_queries(**context):
    """Execute all monitoring queries and store results"""
    pg_hook = PostgresHook(postgres_conn_id='write_to_psql')
    
    results = {}
    for query_name, query in MONITORING_QUERIES.items():
        try:
            df = pg_hook.get_pandas_df(query)
            if not df.empty:
                results[query_name] = df.iloc[0]['metrics']
        except Exception as e:
            print(f"Error executing {query_name}: {str(e)}")
            results[query_name] = {"error": str(e)}
    
    # Store results in XCom
    context['ti'].xcom_push(key='monitoring_results', value=results)
    
    # Save to file for archival
    report_dir = './dags/reports'
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    report_file = f"{report_dir}/monitoring_report_{timestamp}.json"
    
    with open(report_file, 'w') as f:
        json.dump(results, f, indent=2, default=str)
    
    print(f"Monitoring report saved to: {report_file}")
    return report_file

def execute_data_quality_checks(**context):
    """Run data quality checks on all tables"""
    pg_hook = PostgresHook(postgres_conn_id='write_to_psql')
    
    quality_results = {}
    alerts = []
    
    for check_name, query in DATA_QUALITY_QUERIES.items():
        try:
            df = pg_hook.get_pandas_df(query)
            if not df.empty:
                result = df.iloc[0].to_dict()
                quality_results[check_name] = result
                
                # Check for quality issues and create alerts
                if check_name == 'employees_quality':
                    if result['duplicate_emails'] > 0:
                        alerts.append(f"ALERT: {result['duplicate_emails']} duplicate emails found in employees table")
                    if result['invalid_ages'] > 0:
                        alerts.append(f"ALERT: {result['invalid_ages']} invalid ages found in employees table")
                
                elif check_name == 'products_quality':
                    if result['negative_margin_products'] > 0:
                        alerts.append(f"ALERT: {result['negative_margin_products']} products with negative margins")
                    if result['negative_stock'] > 0:
                        alerts.append(f"ALERT: {result['negative_stock']} products with negative stock")
                
                elif check_name == 'sales_quality':
                    if result['failed_payments'] > 10:
                        alerts.append(f"WARNING: {result['failed_payments']} failed payment transactions")
                
        except Exception as e:
            print(f"Error executing {check_name}: {str(e)}")
            quality_results[check_name] = {"error": str(e)}
    
    # Store results
    context['ti'].xcom_push(key='quality_results', value=quality_results)
    context['ti'].xcom_push(key='quality_alerts', value=alerts)
    
    # Log alerts
    if alerts:
        print("\n⚠️ DATA QUALITY ALERTS:")
        for alert in alerts:
            print(f"  - {alert}")
    else:
        print("✅ All data quality checks passed!")
    
    return quality_results

def generate_executive_summary(**context):
    """Generate an executive summary report"""
    ti = context['ti']
    monitoring_results = ti.xcom_pull(key='monitoring_results', task_ids='execute_monitoring_queries')
    quality_results = ti.xcom_pull(key='quality_results', task_ids='execute_data_quality_checks')
    quality_alerts = ti.xcom_pull(key='quality_alerts', task_ids='execute_data_quality_checks')
    
    # Generate summary
    summary = {
        'report_timestamp': datetime.now().isoformat(),
        'report_type': 'Executive Dashboard',
        'period': 'Last 24 Hours',
        'data_quality_status': 'PASS' if not quality_alerts else 'ISSUES DETECTED',
        'alerts': quality_alerts or [],
        'key_metrics': {},
        'recommendations': []
    }
    
    # Extract key metrics from monitoring results
    if monitoring_results:
        # Employee metrics
        if 'employee_metrics' in monitoring_results:
            emp_data = monitoring_results['employee_metrics'].get('total_stats', {})
            summary['key_metrics']['employees'] = {
                'total_active': emp_data.get('active_employees', 0),
                'avg_performance': round(emp_data.get('avg_performance', 0), 2),
                'avg_satisfaction': round(emp_data.get('avg_satisfaction', 0), 2)
            }
        
        # Sales metrics
        if 'sales_metrics' in monitoring_results:
            sales_data = monitoring_results['sales_metrics']
            if 'daily_trends' in sales_data and sales_data['daily_trends']:
                latest_day = sales_data['daily_trends'][0]
                summary['key_metrics']['sales'] = {
                    'daily_revenue': latest_day.get('daily_revenue', 0),
                    'transaction_count': latest_day.get('transaction_count', 0),
                    'avg_transaction_value': round(latest_day.get('avg_transaction_value', 0), 2)
                }
        
        # Support metrics
        if 'support_metrics' in monitoring_results:
            support_data = monitoring_results['support_metrics'].get('ticket_stats', {})
            summary['key_metrics']['support'] = {
                'open_tickets': support_data.get('open_tickets', 0),
                'avg_resolution_time': round(support_data.get('avg_resolution_time', 0), 1),
                'sla_breaches': support_data.get('sla_breaches', 0)
            }
    
    # Generate recommendations based on metrics
    if summary['key_metrics'].get('support', {}).get('sla_breaches', 0) > 5:
        summary['recommendations'].append("High number of SLA breaches detected. Consider increasing support staff.")
    
    if summary['key_metrics'].get('employees', {}).get('avg_satisfaction', 10) < 6:
        summary['recommendations'].append("Employee satisfaction is below target. Review recent policy changes.")
    
    # Save executive summary
    report_dir = './dags/reports'
    os.makedirs(report_dir, exist_ok=True)
    
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    summary_file = f"{report_dir}/executive_summary_{timestamp}.json"
    
    with open(summary_file, 'w') as f:
        json.dump(summary, f, indent=2)
    
    print(f"\n📊 EXECUTIVE SUMMARY")
    print(f"{'='*50}")
    print(f"Report Generated: {summary['report_timestamp']}")
    print(f"Data Quality Status: {summary['data_quality_status']}")
    print(f"Active Alerts: {len(summary['alerts'])}")
    print(f"\nKey Metrics:")
    for category, metrics in summary['key_metrics'].items():
        print(f"\n  {category.upper()}:")
        for metric, value in metrics.items():
            print(f"    - {metric}: {value}")
    
    if summary['recommendations']:
        print(f"\n📌 Recommendations:")
        for rec in summary['recommendations']:
            print(f"  • {rec}")
    
    return summary_file

# Create the monitoring DAG
dag = DAG(
    'data_monitoring_and_reporting',
    default_args=default_args,
    description='Monitor data quality and generate reports',
    schedule_interval='0 */6 * * *',  # Run every 6 hours
    catchup=False,
    max_active_runs=1,
    tags=['monitoring', 'reporting', 'data-quality']
)

# Task to execute monitoring queries
monitoring_task = PythonOperator(
    task_id='execute_monitoring_queries',
    python_callable=execute_monitoring_queries,
    dag=dag
)

# Task to run data quality checks
quality_check_task = PythonOperator(
    task_id='execute_data_quality_checks',
    python_callable=execute_data_quality_checks,
    dag=dag
)

# Task to generate executive summary
summary_task = PythonOperator(
    task_id='generate_executive_summary',
    python_callable=generate_executive_summary,
    dag=dag
)

# Task to create database views for dashboards
create_views_task = PostgresOperator(
    task_id='create_dashboard_views',
    postgres_conn_id='write_to_psql',
    sql="""
        -- Create or replace materialized view for sales dashboard
        CREATE MATERIALIZED VIEW IF NOT EXISTS sales_dashboard_mv AS
        SELECT 
            DATE(order_date) as date,
            sales_channel,
            payment_method,
            COUNT(*) as transactions,
            SUM(total_amount) as revenue,
            AVG(total_amount) as avg_order_value,
            COUNT(DISTINCT customer_id) as unique_customers
        FROM sales_transactions
        WHERE order_date >= CURRENT_DATE - INTERVAL '90 days'
        GROUP BY DATE(order_date), sales_channel, payment_method;
        
        -- Create index for performance
        CREATE INDEX IF NOT EXISTS idx_sales_dashboard_date ON sales_dashboard_mv(date);
        
        -- Refresh the materialized view
        REFRESH MATERIALIZED VIEW CONCURRENTLY sales_dashboard_mv;
        
        -- Create view for employee analytics
        CREATE OR REPLACE VIEW employee_analytics_v AS
        SELECT 
            department,
            job_title,
            COUNT(*) as employee_count,
            AVG(salary) as avg_salary,
            AVG(performance_rating) as avg_performance,
            AVG(satisfaction_score) as avg_satisfaction,
            SUM(CASE WHEN is_manager THEN 1 ELSE 0 END) as manager_count
        FROM employees
        WHERE is_active = true
        GROUP BY department, job_title;
        
        -- Create view for product performance
        CREATE OR REPLACE VIEW product_performance_v AS
        SELECT 
            p.category,
            p.brand,
            COUNT(*) as product_count,
            SUM(p.quantity_in_stock) as total_stock,
            SUM(p.quantity_in_stock * p.unit_price) as stock_value,
            AVG(p.rating) as avg_rating,
            SUM(p.units_sold_30days) as total_sales_30d
        FROM products p
        WHERE p.is_active = true
        GROUP BY p.category, p.brand;
    """,
    dag=dag
)

# Task to archive old reports
def archive_old_reports(**context):
    """Archive reports older than 30 days"""
    report_dir = './dags/reports'
    archive_dir = './dags/reports/archive'
    
    if not os.path.exists(report_dir):
        return
    
    os.makedirs(archive_dir, exist_ok=True)
    
    cutoff_date = datetime.now() - timedelta(days=30)
    archived_count = 0
    
    for filename in os.listdir(report_dir):
        if filename.endswith('.json'):
            file_path = os.path.join(report_dir, filename)
            file_time = datetime.fromtimestamp(os.path.getmtime(file_path))
            
            if file_time < cutoff_date:
                archive_path = os.path.join(archive_dir, filename)
                os.rename(file_path, archive_path)
                archived_count += 1
    
    print(f"Archived {archived_count} old reports")

archive_task = PythonOperator(
    task_id='archive_old_reports',
    python_callable=archive_old_reports,
    dag=dag
)

# Define task dependencies
[monitoring_task, quality_check_task] >> summary_task >> create_views_task >> archive_task

if __name__ == "__main__":
    dag.cli()

3. simple_csv_to_postgres_dag.py: from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime
import pandas as pd
import os

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
}

# Function to read the CSV and generate insert queries
def generate_insert_queries():
    """
    Read CSV file and generate SQL insert statements
    """
    CSV_FILE_PATH = 'sample_files/input.csv'
    
    # Check if file exists
    if not os.path.exists(CSV_FILE_PATH):
        print(f"Warning: {CSV_FILE_PATH} not found. Creating sample data...")
        # Create sample data if file doesn't exist
        sample_data = {
            'id': [1, 2, 3, 4, 5],
            'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Williams', 'Charlie Brown'],
            'age': [30, 25, 35, 28, 32]
        }
        os.makedirs('sample_files', exist_ok=True)
        pd.DataFrame(sample_data).to_csv(CSV_FILE_PATH, index=False)
    
    # Read the CSV file
    df = pd.read_csv(CSV_FILE_PATH)
    print(f"Read {len(df)} rows from {CSV_FILE_PATH}")
    
    # Create directory for SQL files if it doesn't exist
    os.makedirs('./dags/sql', exist_ok=True)
    
    # Create a list of SQL insert queries
    insert_queries = []
    for index, row in df.iterrows():
        # Escape single quotes in names
        name = str(row['name']).replace("'", "''")
        insert_query = f"INSERT INTO sample_table (id, name, age) VALUES ({row['id']}, '{name}', {row['age']});"
        insert_queries.append(insert_query)
    
    # Save queries to a file for the PostgresOperator to execute
    sql_file_path = './dags/sql/insert_queries.sql'
    with open(sql_file_path, 'w') as f:
        for query in insert_queries:
            f.write(f"{query}\n")
    
    print(f"Generated {len(insert_queries)} insert queries in {sql_file_path}")
    return len(insert_queries)

# Define the DAG
with DAG('simple_csv_to_postgres_dag',
         default_args=default_args,
         description='Simple DAG to load CSV data into PostgreSQL',
         schedule_interval='@once',
         catchup=False,
         tags=['simple', 'csv', 'postgres']) as dag:

    # Task to create a PostgreSQL table
    create_table = PostgresOperator(
        task_id='create_table',
        postgres_conn_id='write_to_psql',
        sql="""
        DROP TABLE IF EXISTS sample_table;
        CREATE TABLE sample_table (
            id SERIAL PRIMARY KEY,
            name VARCHAR(50),
            age INT,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
        CREATE INDEX idx_sample_name ON sample_table(name);
        """,
        autocommit=True
    )

    # Task to generate insert queries from CSV
    generate_queries = PythonOperator(
        task_id='generate_insert_queries',
        python_callable=generate_insert_queries
    )

    # Task to run the generated SQL queries
    run_insert_queries = PostgresOperator(
        task_id='run_insert_queries',
        postgres_conn_id='write_to_psql',
        sql='sql/insert_queries.sql',
        autocommit=True
    )
    
    # Task to verify data was inserted
    verify_data = PostgresOperator(
        task_id='verify_data',
        postgres_conn_id='write_to_psql',
        sql="""
        SELECT COUNT(*) as total_records, 
               MIN(age) as min_age, 
               MAX(age) as max_age,
               AVG(age) as avg_age
        FROM sample_table;
        """,
        autocommit=True
    )

    # Define task dependencies
    create_table >> generate_queries >> run_insert_queries >> verify_data

4. csv_processor.py: from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import pandas as pd
import os
import logging

# Define default arguments
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Configure logging
logger = logging.getLogger(__name__)

# Function to process large CSV files in chunks
def process_csv_in_chunks(csv_file, table_name, **context):
    """
    Process large CSV files in chunks to manage memory efficiently
    """
    CSV_FILE_PATH = f'sample_files/{csv_file}'
    CHUNK_SIZE = 10000  # Process 10,000 rows at a time
    
    # Check if file exists
    if not os.path.exists(CSV_FILE_PATH):
        logger.warning(f"File {CSV_FILE_PATH} not found")
        raise FileNotFoundError(f"CSV file not found: {CSV_FILE_PATH}")
    
    # Create directory for SQL files if it doesn't exist
    os.makedirs('./dags/sql', exist_ok=True)
    
    # Open SQL file for writing
    sql_file_path = f'./dags/sql/{table_name}_insert.sql'
    
    total_rows = 0
    with open(sql_file_path, 'w') as sql_file:
        # Read CSV in chunks
        for chunk_num, chunk in enumerate(pd.read_csv(CSV_FILE_PATH, chunksize=CHUNK_SIZE)):
            rows_in_chunk = len(chunk)
            total_rows += rows_in_chunk
            logger.info(f"Processing chunk {chunk_num + 1} with {rows_in_chunk} rows...")
            
            # Generate insert queries for this chunk
            for index, row in chunk.iterrows():
                # Build dynamic insert query based on columns
                columns = ', '.join(chunk.columns)
                values = []
                
                for col in chunk.columns:
                    val = row[col]
                    if pd.isna(val):
                        values.append('NULL')
                    elif isinstance(val, str):
                        # Escape single quotes in strings
                        escaped_val = str(val).replace("'", "''")
                        values.append(f"'{escaped_val}'")
                    elif isinstance(val, bool):
                        values.append('TRUE' if val else 'FALSE')
                    else:
                        values.append(str(val))
                
                insert_query = f"INSERT INTO {table_name} ({columns}) VALUES ({', '.join(values)}) ON CONFLICT DO NOTHING;"
                sql_file.write(f"{insert_query}\n")
            
            # Log progress every 5 chunks
            if (chunk_num + 1) % 5 == 0:
                logger.info(f"Processed {total_rows} rows so far...")
        
        logger.info(f"✅ Generated SQL file: {sql_file_path} with {total_rows} insert statements")
    
    # Store file path and row count in XCom for next task
    context['ti'].xcom_push(key=f'{table_name}_sql_path', value=sql_file_path)
    context['ti'].xcom_push(key=f'{table_name}_row_count', value=total_rows)
    
    return total_rows

# Function to verify data load
def verify_data_load(table_name, **context):
    """
    Verify that data was loaded successfully
    """
    ti = context['ti']
    expected_rows = ti.xcom_pull(key=f'{table_name}_row_count')
    
    logger.info(f"Expected {expected_rows} rows to be loaded into {table_name}")
    # In a real scenario, you would query the database to verify
    
    return f"Data load verified for {table_name}"

# Function to create tables dynamically
def get_create_table_sql(table_name):
    """
    Generate CREATE TABLE SQL based on table name
    """
    table_definitions = {
        'employees': """
            DROP TABLE IF EXISTS employees CASCADE;
            CREATE TABLE employees (
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Create indexes for better query performance
            CREATE INDEX idx_employees_department ON employees(department);
            CREATE INDEX idx_employees_email ON employees(email);
            CREATE INDEX idx_employees_hire_date ON employees(hire_date);
        """,
        
        'products': """
            DROP TABLE IF EXISTS products CASCADE;
            CREATE TABLE products (
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
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            -- Create indexes
            CREATE INDEX idx_products_category ON products(category);
            CREATE INDEX idx_products_sku ON products(sku);
            CREATE INDEX idx_products_brand ON products(brand);
        """,
        
        'sales_transactions': """
            DROP TABLE IF EXISTS sales_transactions CASCADE;
            CREATE TABLE sales_transactions (
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
                refund_amount DECIMAL(12,2),
                is_gift BOOLEAN,
                loyalty_points_earned INTEGER,
                created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            );
            
            CREATE INDEX idx_sales_order_date ON sales_transactions(order_date);
            CREATE INDEX idx_sales_customer ON sales_transactions(customer_id);
            CREATE INDEX idx_sales_status ON sales_transactions(order_status);
        """
    }
    
    # Return the specific table definition or a generic one
    return table_definitions.get(table_name, f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            id SERIAL PRIMARY KEY,
            data JSONB,
            created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
        );
    """)

# Define the DAG
with DAG('large_csv_processor_dag',
         default_args=default_args,
         description='Process large CSV files and load into PostgreSQL',
         schedule_interval='@daily',
         catchup=False,
         max_active_runs=1,
         tags=['bulk', 'csv', 'postgres', 'production']) as dag:

    # Dictionary of CSV files and their corresponding table names
    csv_files = {
        'employees_large.csv': 'employees',
        'product_inventory.csv': 'products',
        'sales_transactions.csv': 'sales_transactions',
        # Add more files as they become available
    }
    
    # Create a start task
    start = PythonOperator(
        task_id='start',
        python_callable=lambda: logger.info("Starting CSV processing pipeline")
    )
    
    # Create tasks dynamically for each CSV file
    for csv_file, table_name in csv_files.items():
        
        # Task to create table
        create_table_task = PostgresOperator(
            task_id=f'create_{table_name}_table',
            postgres_conn_id='write_to_psql',
            sql=get_create_table_sql(table_name),
            autocommit=True
        )
        
        # Task to process CSV and generate SQL
        process_csv_task = PythonOperator(
            task_id=f'process_{table_name}_csv',
            python_callable=process_csv_in_chunks,
            op_kwargs={
                'csv_file': csv_file,
                'table_name': table_name
            },
            provide_context=True
        )
        
        # Task to execute SQL inserts
        insert_data_task = PostgresOperator(
            task_id=f'insert_{table_name}_data',
            postgres_conn_id='write_to_psql',
            sql=f'sql/{table_name}_insert.sql',
            autocommit=True
        )
        
        # Task to analyze table for query optimization
        analyze_table_task = PostgresOperator(
            task_id=f'analyze_{table_name}_table',
            postgres_conn_id='write_to_psql',
            sql=f"ANALYZE {table_name};",
            autocommit=True
        )
        
        # Task to verify data load
        verify_task = PythonOperator(
            task_id=f'verify_{table_name}_load',
            python_callable=verify_data_load,
            op_kwargs={'table_name': table_name},
            provide_context=True
        )
        
        # Define task dependencies
        start >> create_table_task >> process_csv_task >> insert_data_task >> analyze_table_task >> verify_task
    
    # Create an end task
    end = PythonOperator(
        task_id='end',
        python_callable=lambda: logger.info("CSV processing pipeline completed successfully"),
        trigger_rule='none_failed'  # Run even if some tables were skipped
    )
    
    # Connect all verify tasks to end
    for csv_file, table_name in csv_files.items():
        dag.get_task(f'verify_{table_name}_load') >> end

5. simple_test_dag.py: from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 1, 1),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

def print_hello():
    print("Hello from Airflow!")
    return "Hello World"

def print_date(**context):
    print(f"Execution date: {context['ds']}")
    print(f"Current time: {datetime.now()}")
    return "Date printed successfully"

with DAG(
    'simple_test_dag',
    default_args=default_args,
    description='A simple test DAG to verify Airflow is working',
    schedule_interval='@once',
    catchup=False,
    tags=['test', 'example']
) as dag:
    
    hello_task = PythonOperator(
        task_id='print_hello',
        python_callable=print_hello
    )
    
    date_task = PythonOperator(
        task_id='print_date',
        python_callable=print_date,
        provide_context=True
    )
    
    # Set task dependencies
    hello_task >> date_task

the other folder is:
2. scripts. this also has these files:
1. generate_sample_data.py: #!/usr/bin/env python3
"""
Generate large sample CSV files for Apache Airflow pipeline testing
This script creates 6 CSV files with a total of 875,000 records
"""

import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os
import sys

# Set random seed for reproducibility
np.random.seed(42)
random.seed(42)

# Create sample_files directory if it doesn't exist
os.makedirs('sample_files', exist_ok=True)

# Helper functions
def generate_phone():
    """Generate a random phone number"""
    return f"({random.randint(200, 999)}){random.randint(100, 999)}-{random.randint(1000, 9999)}"

def generate_address():
    """Generate a random address"""
    street_names = ['Main', 'Oak', 'Pine', 'Elm', 'Park', 'Lake', 'Hill', 'Maple', 'Cedar', 'Washington']
    street_types = ['St', 'Ave', 'Rd', 'Blvd', 'Dr', 'Ln', 'Way', 'Ct', 'Pl']
    return f"{random.randint(1, 9999)} {random.choice(street_names)} {random.choice(street_types)}"

def generate_employees(num_rows=300000):
    """Generate employee data with 30 columns"""
    print(f"\nGenerating {num_rows:,} employee records...")
    
    # Data arrays for realistic content
    first_names = ['John', 'Jane', 'Michael', 'Sarah', 'David', 'Emily', 'Robert', 'Lisa', 'James', 'Mary',
                   'William', 'Patricia', 'Richard', 'Jennifer', 'Charles', 'Linda', 'Joseph', 'Barbara',
                   'Thomas', 'Susan', 'Christopher', 'Jessica', 'Daniel', 'Karen', 'Paul', 'Nancy', 'Mark',
                   'Betty', 'Donald', 'Helen', 'George', 'Sandra', 'Kenneth', 'Donna', 'Steven', 'Carol']
    
    last_names = ['Smith', 'Johnson', 'Williams', 'Brown', 'Jones', 'Garcia', 'Miller', 'Davis', 'Rodriguez',
                  'Martinez', 'Hernandez', 'Lopez', 'Gonzalez', 'Wilson', 'Anderson', 'Thomas', 'Taylor',
                  'Moore', 'Jackson', 'Martin', 'Lee', 'Perez', 'Thompson', 'White', 'Harris', 'Sanchez',
                  'Clark', 'Ramirez', 'Lewis', 'Robinson', 'Walker', 'Young', 'Allen', 'King', 'Wright']
    
    departments = ['Sales', 'Marketing', 'IT', 'HR', 'Finance', 'Operations', 'Customer Service', 
                   'R&D', 'Legal', 'Administration', 'Engineering', 'Product', 'Quality Assurance']
    
    job_titles = ['Manager', 'Senior Manager', 'Director', 'Analyst', 'Senior Analyst', 'Specialist',
                  'Coordinator', 'Executive', 'Associate', 'Consultant', 'Engineer', 'Developer',
                  'Designer', 'Architect', 'Administrator', 'Supervisor', 'Lead', 'Principal']
    
    cities = ['New York', 'Los Angeles', 'Chicago', 'Houston', 'Phoenix', 'Philadelphia', 'San Antonio',
              'San Diego', 'Dallas', 'San Jose', 'Austin', 'Jacksonville', 'Fort Worth', 'Columbus']
    
    states = ['NY', 'CA', 'IL', 'TX', 'AZ', 'PA', 'TX', 'CA', 'TX', 'CA', 'TX', 'FL', 'TX', 'OH']
    
    education_levels = ['High School', 'Associate', 'Bachelor', 'Master', 'PhD', 'Professional']
    marital_status = ['Single', 'Married', 'Divorced', 'Widowed', 'Separated']
    
    # Generate data
    data = []
    for i in range(num_rows):
        employee_id = i + 1
        first_name = random.choice(first_names)
        last_name = random.choice(last_names)
        
        row = {
            'employee_id': employee_id,
            'first_name': first_name,
            'last_name': last_name,
            'age': random.randint(22, 67),
            'gender': random.choice(['M', 'F', 'Other']),
            'email': f"{first_name.lower()}.{last_name.lower()}{random.randint(1, 999)}@company.com",
            'phone': generate_phone(),
            'address': generate_address(),
            'city': random.choice(cities),
            'state': random.choice(states),
            'zip_code': str(random.randint(10000, 99999)),
            'country': random.choice(['USA', 'USA', 'USA', 'Canada', 'Mexico']),
            'hire_date': (datetime.now() - timedelta(days=random.randint(0, 5000))).strftime('%Y-%m-%d'),
            'department': random.choice(departments),
            'job_title': random.choice(job_titles),
            'salary': random.randint(30000, 250000),
            'bonus_percentage': round(random.uniform(0, 30), 2),
            'years_experience': random.randint(0, 40),
            'education_level': random.choice(education_levels),
            'performance_rating': round(random.uniform(1, 5), 2),
            'satisfaction_score': round(random.uniform(1, 10), 2),
            'attendance_rate': round(random.uniform(85, 100), 2),
            'training_hours': random.randint(0, 200),
            'certifications': random.randint(0, 10),
            'is_manager': random.choice([True, False]),
            'team_size': random.randint(0, 50),
            'remote_work_days': random.randint(0, 5),
            'marital_status': random.choice(marital_status),
            'dependents': random.randint(0, 6),
            'emergency_contact': f"{random.choice(first_names)} {random.choice(last_names)}"
        }
        data.append(row)
        
        if (i + 1) % 50000 == 0:
            print(f"  Generated {i + 1:,} employee records...")
    
    # Create DataFrame and save
    df = pd.DataFrame(data)
    df.to_csv('sample_files/employees_large.csv', index=False)
    print(f"✅ Created employees_large.csv with {len(df):,} rows and {len(df.columns)} columns")
    return len(df)

def generate_products(num_rows=100000):
    """Generate product inventory data with 30 columns"""
    print(f"\nGenerating {num_rows:,} product records...")
    
    categories = ['Electronics', 'Clothing', 'Home & Garden', 'Sports', 'Books', 'Toys', 'Food & Beverage', 
                  'Health & Beauty', 'Automotive', 'Office Supplies']
    brands = ['TechCorp', 'StyleMax', 'HomeEase', 'SportPro', 'BookWorld', 'FunTime', 'GourmetLife', 
              'BeautyPlus', 'AutoZone', 'OfficeHub']
    conditions = ['New', 'Refurbished', 'Used - Like New', 'Used - Good', 'Used - Acceptable']
    warehouses = ['WH-North', 'WH-South', 'WH-East', 'WH-West', 'WH-Central']
    suppliers = ['Supplier A', 'Supplier B', 'Supplier C', 'Supplier D', 'Supplier E', 'Supplier F']
    
    data = []
    for i in range(num_rows):
        product_id = f"PROD{str(i + 1).zfill(6)}"
        category = random.choice(categories)
        brand = random.choice(brands)
        
        row = {
            'product_id': product_id,
            'sku': f"SKU{random.randint(100000, 999999)}",
            'product_name': f"{brand} {category} Item {random.randint(1, 1000)}",
            'category': category,
            'brand': brand,
            'description': f"High-quality {category.lower()} product from {brand}",
            'unit_price': round(random.uniform(9.99, 999.99), 2),
            'cost_price': round(random.uniform(5.00, 500.00), 2),
            'quantity_in_stock': random.randint(0, 5000),
            'reorder_level': random.randint(10, 100),
            'reorder_quantity': random.randint(50, 500),
            'warehouse_location': random.choice(warehouses),
            'supplier': random.choice(suppliers),
            'weight_kg': round(random.uniform(0.1, 50.0), 2),
            'dimensions_cm': f"{random.randint(5, 100)}x{random.randint(5, 100)}x{random.randint(5, 100)}",
            'condition': random.choice(conditions),
            'manufactured_date': (datetime.now() - timedelta(days=random.randint(0, 730))).strftime('%Y-%m-%d'),
            'expiry_date': 'N/A' if category not in ['Food & Beverage', 'Health & Beauty'] else 
                          (datetime.now() + timedelta(days=random.randint(365, 1095))).strftime('%Y-%m-%d'),
            'last_restocked': (datetime.now() - timedelta(days=random.randint(0, 30))).strftime('%Y-%m-%d'),
            'units_sold_30days': random.randint(0, 500),
            'units_sold_90days': random.randint(0, 1500),
            'units_sold_365days': random.randint(0, 6000),
            'rating': round(random.uniform(1.0, 5.0), 1),
            'review_count': random.randint(0, 2000),
            'is_active': random.choice([True, True, True, True, False]),  # 80% active
            'discount_percentage': round(random.uniform(0, 50), 1),
            'tax_rate': round(random.uniform(0, 15), 2),
            'shipping_class': random.choice(['Standard', 'Express', 'Overnight', 'Freight']),
            'country_of_origin': random.choice(['USA', 'China', 'Germany', 'Japan', 'Mexico', 'Canada', 'India']),
            'barcode': str(random.randint(100000000, 999999999)),
            'minimum_age': random.choice([0, 0, 0, 13, 18]) if category == 'Toys' else 0
        }
        data.append(row)
        
        if (i + 1) % 20000 == 0:
            print(f"  Generated {i + 1:,} product records...")
    
    df = pd.DataFrame(data)
    df.to_csv('sample_files/product_inventory.csv', index=False)
    print(f"✅ Created product_inventory.csv with {len(df):,} rows and {len(df.columns)} columns")
    return len(df)

def generate_sales_transactions(num_rows=150000):
    """Generate sales transaction data with 30 columns"""
    print(f"\nGenerating {num_rows:,} sales transaction records...")
    
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer', 'Cryptocurrency', 'Gift Card']
    sales_channels = ['Online', 'In-Store', 'Mobile App', 'Phone', 'Social Media', 'Marketplace']
    customer_types = ['Regular', 'Premium', 'VIP', 'New', 'Returning']
    regions = ['North America', 'Europe', 'Asia', 'South America', 'Africa', 'Oceania']
    order_statuses = ['Delivered', 'Shipped', 'Processing', 'Cancelled', 'Refunded']
    
    data = []
    for i in range(num_rows):
        transaction_id = f"TRX{str(i + 1).zfill(8)}"
        
        # Generate multiple products per transaction
        num_products = random.randint(1, 5)
        product_ids = [f"PROD{random.randint(1, 100000):06d}" for _ in range(num_products)]
        product_names = [f"Product {random.randint(1, 1000)}" for _ in range(num_products)]
        quantities = [random.randint(1, 10) for _ in range(num_products)]
        unit_prices = [round(random.uniform(9.99, 299.99), 2) for _ in range(num_products)]
        
        subtotal = sum(q * p for q, p in zip(quantities, unit_prices))
        tax_amount = round(subtotal * 0.0875, 2)
        shipping_cost = round(random.uniform(0, 25), 2)
        discount_amount = round(subtotal * random.uniform(0, 0.2), 2)
        total_amount = round(subtotal + tax_amount + shipping_cost - discount_amount, 2)
        
        row = {
            'transaction_id': transaction_id,
            'order_date': (datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S'),
            'customer_id': f"CUST{random.randint(1, 50000):06d}",
            'customer_name': f"{random.choice(['John', 'Jane', 'Michael', 'Sarah'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown'])}",
            'customer_email': f"customer{random.randint(1, 50000)}@email.com",
            'customer_type': random.choice(customer_types),
            'product_ids': ';'.join(product_ids),
            'product_names': ';'.join(product_names),
            'quantities': ';'.join(map(str, quantities)),
            'unit_prices': ';'.join(map(str, unit_prices)),
            'subtotal': subtotal,
            'tax_amount': tax_amount,
            'shipping_cost': shipping_cost,
            'discount_amount': discount_amount,
            'total_amount': total_amount,
            'payment_method': random.choice(payment_methods),
            'payment_status': random.choice(['Completed', 'Completed', 'Completed', 'Pending', 'Failed']),
            'order_status': random.choice(order_statuses),
            'sales_channel': random.choice(sales_channels),
            'sales_rep_id': f"REP{random.randint(1, 100):03d}",
            'sales_rep_name': f"{random.choice(['Tom', 'Amy', 'Jack', 'Emma'])} {random.choice(['Wilson', 'Davis', 'Moore'])}",
            'shipping_address': f"{generate_address()}, {random.choice(['New York, NY', 'Los Angeles, CA', 'Chicago, IL'])} {random.randint(10000, 99999)}",
            'billing_address': f"{generate_address()}, {random.choice(['Houston, TX', 'Phoenix, AZ', 'Dallas, TX'])} {random.randint(10000, 99999)}",
            'region': random.choice(regions),
            'country': random.choice(['USA', 'Canada', 'UK', 'Germany', 'France', 'Japan']),
            'currency': random.choice(['USD', 'EUR', 'GBP', 'CAD', 'JPY']),
            'exchange_rate': round(random.uniform(0.8, 1.2), 4),
            'notes': '' if random.random() > 0.2 else 'Special delivery instructions',
            'refund_amount': total_amount if row.get('order_status') == 'Refunded' else 0,
            'is_gift': random.choice([False, False, False, True]),
            'loyalty_points_earned': int(total_amount * 10)
        }
        data.append(row)
        
        if (i + 1) % 30000 == 0:
            print(f"  Generated {i + 1:,} sales transaction records...")
    
    df = pd.DataFrame(data)
    df.to_csv('sample_files/sales_transactions.csv', index=False)
    print(f"✅ Created sales_transactions.csv with {len(df):,} rows and {len(df.columns)} columns")
    return len(df)

def generate_support_tickets(num_rows=75000):
    """Generate customer support ticket data with 30 columns"""
    print(f"\nGenerating {num_rows:,} support ticket records...")
    
    categories = ['Technical Issue', 'Billing', 'Product Inquiry', 'Shipping', 'Returns', 'Account', 'Feature Request', 'Complaint']
    priorities = ['Low', 'Medium', 'High', 'Critical']
    statuses = ['Open', 'In Progress', 'Pending Customer', 'Resolved', 'Closed', 'Escalated']
    channels = ['Email', 'Phone', 'Chat', 'Social Media', 'Web Form', 'Mobile App']
    departments = ['Level 1 Support', 'Level 2 Support', 'Technical Team', 'Billing Team', 'Management']
    
    data = []
    for i in range(num_rows):
        ticket_id = f"TICKET{str(i + 1).zfill(7)}"
        status = random.choice(statuses)
        priority = random.choice(priorities)
        
        row = {
            'ticket_id': ticket_id,
            'created_date': (datetime.now() - timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d %H:%M:%S'),
            'customer_id': f"CUST{random.randint(1, 50000):06d}",
            'customer_name': f"{random.choice(['John', 'Jane', 'Michael', 'Sarah'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown'])}",
            'customer_email': f"customer{random.randint(1, 50000)}@email.com",
            'category': random.choice(categories),
            'subcategory': f"{random.choice(categories)} - Type {random.randint(1, 5)}",
            'priority': priority,
            'status': status,
            'subject': f"Issue with order #{random.randint(10000, 99999)}",
            'description': "Customer reported an issue that requires attention and resolution.",
            'channel': random.choice(channels),
            'assigned_to': f"AGENT{random.randint(1, 50):03d}",
            'department': random.choice(departments),
            'first_response_time_hours': round(random.uniform(0.1, 24), 2),
            'resolution_time_hours': round(random.uniform(1, 168), 2) if status in ['Resolved', 'Closed'] else 0,
            'number_of_interactions': random.randint(1, 20),
            'satisfaction_rating': round(random.uniform(1, 5), 1) if status in ['Resolved', 'Closed'] else 0,
            'tags': f"{random.choice(categories).lower()},{priority.lower()},{random.choice(channels).lower()}",
            'related_order_id': f"TRX{random.randint(1, 150000):08d}" if random.random() < 0.6 else '',
            'product_id': f"PROD{random.randint(1, 100000):06d}" if random.random() < 0.4 else '',
            'escalated': True if priority == 'Critical' or random.random() < 0.1 else False,
            'reopened': random.choice([False, False, False, False, True]),
            'agent_notes': "Initial investigation completed. Working on resolution.",
            'resolution_notes': "Issue resolved successfully." if status in ['Resolved', 'Closed'] else '',
            'sla_breach': random.choice([False, False, False, True]),
            'response_sla_hours': 1 if priority == 'Critical' else 4 if priority == 'High' else 24,
            'resolution_sla_hours': 4 if priority == 'Critical' else 24 if priority == 'High' else 72,
            'customer_lifetime_value': round(random.uniform(100, 10000), 2),
            'is_vip_customer': random.choice([False, False, False, True]),
            'language': random.choice(['English', 'Spanish', 'French', 'German', 'Japanese'])
        }
        data.append(row)
        
        if (i + 1) % 15000 == 0:
            print(f"  Generated {i + 1:,} support ticket records...")
    
    df = pd.DataFrame(data)
    df.to_csv('sample_files/customer_support_tickets.csv', index=False)
    print(f"✅ Created customer_support_tickets.csv with {len(df):,} rows and {len(df.columns)} columns")
    return len(df)

def generate_web_analytics(num_rows=200000):
    """Generate web analytics data with 30 columns"""
    print(f"\nGenerating {num_rows:,} web analytics records...")
    
    pages = ['/home', '/products', '/about', '/contact', '/blog', '/checkout', '/cart', '/login', '/register', '/search']
    sources = ['Organic Search', 'Direct', 'Social Media', 'Email', 'Paid Search', 'Referral', 'Display Ads']
    devices = ['Desktop', 'Mobile', 'Tablet']
    browsers = ['Chrome', 'Safari', 'Firefox', 'Edge', 'Opera', 'Samsung Internet']
    countries = ['USA', 'UK', 'Canada', 'Germany', 'France', 'Japan', 'Australia', 'Brazil', 'India', 'Mexico']
    os_list = ['Windows', 'macOS', 'iOS', 'Android', 'Linux', 'ChromeOS']
    campaigns = ['Summer Sale', 'Black Friday', 'New Year', 'Spring Collection', 'Flash Sale', 'Newsletter']
    
    data = []
    for i in range(num_rows):
        session_id = f"SESSION{str(i + 1).zfill(10)}"
        device = random.choice(devices)
        pages_viewed = random.randint(1, 20)
        
        row = {
            'session_id': session_id,
            'user_id': f"USER{random.randint(1, 100000):08d}" if random.random() < 0.7 else 'anonymous',
            'timestamp': (datetime.now() - timedelta(hours=random.randint(0, 720))).strftime('%Y-%m-%d %H:%M:%S'),
            'page_url': random.choice(pages),
            'referrer_url': random.choice(['https://google.com', 'https://facebook.com', 'https://twitter.com', 'direct', '']),
            'source': random.choice(sources),
            'medium': random.choice(['organic', 'cpc', 'referral', 'email', 'social']),
            'campaign': random.choice(campaigns) if random.random() < 0.3 else '',
            'device_type': device,
            'browser': random.choice(browsers),
            'operating_system': random.choice(os_list),
            'screen_resolution': '1920x1080' if device == 'Desktop' else '375x667' if device == 'Mobile' else '768x1024',
            'country': random.choice(countries),
            'city': random.choice(['New York', 'London', 'Toronto', 'Berlin', 'Paris', 'Tokyo', 'Sydney']),
            'language': random.choice(['en-US', 'en-GB', 'es-ES', 'fr-FR', 'de-DE', 'ja-JP']),
            'session_duration_seconds': random.randint(1, 1800),
            'pages_viewed': pages_viewed,
            'bounce': True if pages_viewed == 1 else False,
            'conversion': random.choice([False, False, False, True]),
            'conversion_value': round(random.uniform(10, 500), 2) if row.get('conversion') else 0,
            'events_triggered': random.randint(0, 50),
            'click_through_rate': round(random.uniform(0, 10), 2),
            'scroll_depth_percentage': random.randint(0, 100),
            'form_submissions': random.randint(0, 3),
            'downloads': random.randint(0, 5),
            'video_plays': random.randint(0, 10),
            'add_to_cart': random.randint(0, 5),
            'purchases': random.randint(0, 2),
            'new_vs_returning': random.choice(['New', 'Returning']),
            'user_agent': f"Mozilla/5.0 ({random.choice(os_list)}) {random.choice(browsers)}/{random.randint(70, 100)}.0",
            'ip_address_hash': f"hash_{random.randint(100000, 999999)}",
            'session_quality_score': round(random.uniform(0, 100), 2)
        }
        data.append(row)
        
        if (i + 1) % 40000 == 0:
            print(f"  Generated {i + 1:,} web analytics records...")
    
    df = pd.DataFrame(data)
    df.to_csv('sample_files/website_analytics.csv', index=False)
    print(f"✅ Created website_analytics.csv with {len(df):,} rows and {len(df.columns)} columns")
    return len(df)

def generate_healthcare_records(num_rows=50000):
    """Generate healthcare record data with 30 columns"""
    print(f"\nGenerating {num_rows:,} healthcare records...")
    
    departments = ['Emergency', 'Cardiology', 'Neurology', 'Orthopedics', 'Pediatrics', 'Oncology', 'General Medicine', 'Surgery']
    diagnoses = ['Hypertension', 'Diabetes Type 2', 'Asthma', 'Arthritis', 'Depression', 'Anxiety', 'Back Pain', 'Migraine']
    blood_types = ['A+', 'A-', 'B+', 'B-', 'AB+', 'AB-', 'O+', 'O-']
    admission_types = ['Emergency', 'Elective', 'Urgent', 'Routine']
    discharge_dispositions = ['Home', 'Home Health', 'Skilled Nursing', 'Rehab', 'AMA']
    insurance_providers = ['Blue Cross', 'Aetna', 'Cigna', 'United Health', 'Kaiser', 'Anthem', 'Humana']
    
    data = []
    for i in range(num_rows):
        patient_id = f"PAT{str(i + 1).zfill(8)}"
        admission_date = datetime.now() - timedelta(days=random.randint(0, 365))
        stay_days = random.randint(1, 14)
        discharge_date = admission_date + timedelta(days=stay_days)
        birth_year = datetime.now().year - random.randint(20, 80)
        
        total_charges = round(random.uniform(500, 50000), 2)
        insurance_covered = round(total_charges * random.uniform(0.6, 0.9), 2)
        copay = round(random.uniform(20, 200), 2)
        patient_balance = round(total_charges - insurance_covered + copay, 2)
        
        row = {
            'patient_id': patient_id,
            'admission_date': admission_date.strftime('%Y-%m-%d'),
            'discharge_date': discharge_date.strftime('%Y-%m-%d'),
            'patient_name': f"{random.choice(['John', 'Jane', 'Michael', 'Sarah'])} {random.choice(['Smith', 'Johnson', 'Williams', 'Brown'])}",
            'date_of_birth': f"{birth_year}-{random.randint(1, 12):02d}-{random.randint(1, 28):02d}",
            'age': datetime.now().year - birth_year,
            'gender': random.choice(['Male', 'Female', 'Other']),
            'blood_type': random.choice(blood_types),
            'height_cm': random.randint(150, 200),
            'weight_kg': round(random.uniform(45, 120), 1),
            'bmi': round(random.uniform(18, 35), 1),
            'department': random.choice(departments),
            'primary_diagnosis': random.choice(diagnoses),
            'secondary_diagnosis': random.choice(diagnoses) if random.random() < 0.4 else '',
            'attending_physician': f"Dr. {random.choice(['Smith', 'Johnson', 'Williams', 'Brown', 'Davis'])}",
            'referring_physician': f"Dr. {random.choice(['Miller', 'Wilson', 'Moore', 'Taylor'])}" if random.random() < 0.6 else '',
            'procedures_performed': f"{random.choice(['Blood Test', 'X-Ray', 'MRI', 'CT Scan'])};{random.choice(['ECG', 'Ultrasound'])}",
            'medications_prescribed': f"{random.choice(['Ibuprofen', 'Amoxicillin', 'Lisinopril'])};{random.choice(['Metformin', 'Omeprazole'])}",
            'lab_results': random.choice(['Normal', 'Abnormal', 'Pending']),
            'vital_signs': f"BP:{random.randint(110, 140)}/{random.randint(70, 90)};HR:{random.randint(60, 100)};Temp:{round(random.uniform(97, 99), 1)}F",
            'insurance_provider': random.choice(insurance_providers),
            'insurance_id': f"INS{random.randint(100000000, 999999999)}",
            'copay_amount': copay,
            'total_charges': total_charges,
            'insurance_covered': insurance_covered,
            'patient_balance': patient_balance,
            'admission_type': random.choice(admission_types),
            'discharge_disposition': random.choice(discharge_dispositions),
            'length_of_stay_days': stay_days,
            'readmission_risk': random.choice(['Low', 'Medium', 'High']),
            'patient_satisfaction': round(random.uniform(1, 5), 1),
            'follow_up_required': random.choice([True, True, True, False])
        }
        data.append(row)
        
        if (i + 1) % 10000 == 0:
            print(f"  Generated {i + 1:,} healthcare records...")
    
    df = pd.DataFrame(data)
    df.to_csv('sample_files/healthcare_records.csv', index=False)
    print(f"✅ Created healthcare_records.csv with {len(df):,} rows and {len(df.columns)} columns")
    return len(df)

def generate_small_test_file():
    """Generate a small test CSV file"""
    print("\nGenerating small test file...")
    
    data = {
        'id': [1, 2, 3, 4, 5],
        'name': ['John Doe', 'Jane Smith', 'Bob Johnson', 'Alice Williams', 'Charlie Brown'],
        'age': [30, 25, 35, 28, 32]
    }
    
    df = pd.DataFrame(data)
    df.to_csv('sample_files/input.csv', index=False)
    print("✅ Created input.csv with 5 rows (test file)")
    return len(df)

def main():
    """Main function to generate all CSV files"""
    print("="*60)
    print("Apache Airflow Sample Data Generator")
    print("="*60)
    print(f"Starting data generation at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    
    total_rows = 0
    start_time = datetime.now()
    
    # Generate all CSV files
    total_rows += generate_employees(300000)
    total_rows += generate_products(100000)
    total_rows += generate_sales_transactions(150000)
    total_rows += generate_support_tickets(75000)
    total_rows += generate_web_analytics(200000)
    total_rows += generate_healthcare_records(50000)
    total_rows += generate_small_test_file()
    
    end_time = datetime.now()
    duration = (end_time - start_time).total_seconds()
    
    print("\n" + "="*60)
    print("✅ DATA GENERATION COMPLETE!")
    print("="*60)
    print(f"Total records generated: {total_rows:,}")
    print(f"Total files created: 7")
    print(f"Time taken: {duration:.2f} seconds")
    print(f"Files location: ./sample_files/")
    print("\nGenerated files:")
    print("  1. employees_large.csv         - 300,000 rows")
    print("  2. product_inventory.csv       - 100,000 rows")
    print("  3. sales_transactions.csv      - 150,000 rows")
    print("  4. customer_support_tickets.csv - 75,000 rows")
    print("  5. website_analytics.csv       - 200,000 rows")
    print("  6. healthcare_records.csv      - 50,000 rows")
    print("  7. input.csv                   - 5 rows (test file)")
    print("="*60)

if __name__ == "__main__":
    main()

2. backup_postgres.sh: #!/bin/bash

# PostgreSQL Backup Script for Apache Airflow
# This script performs automated backups of the PostgreSQL database
# Author: Derek
# Date: 2025

# Configuration
BACKUP_DIR="./backups"
TIMESTAMP=$(date +%Y%m%d_%H%M%S)
DATE=$(date +%Y-%m-%d)
DB_NAME="airflow"
DB_USER="airflow"
DB_PASSWORD="airflow"
CONTAINER_NAME="airflow-csv-postgres-postgres-1"  # Update if your container name is different
KEEP_DAYS=7  # Number of days to keep backups
LOG_FILE="$BACKUP_DIR/backup_log.txt"

# Color codes for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to log messages
log_message() {
    echo "[$(date '+%Y-%m-%d %H:%M:%S')] $1" | tee -a "$LOG_FILE"
}

# Function to check if container is running
check_container() {
    if ! docker ps --format '{{.Names}}' | grep -q "^${CONTAINER_NAME}$"; then
        echo -e "${RED}Error: PostgreSQL container '${CONTAINER_NAME}' is not running!${NC}"
        echo "Available containers:"
        docker ps --format 'table {{.Names}}\t{{.Status}}'
        exit 1
    fi
}

# Function to create backup
create_backup() {
    local backup_file="$BACKUP_DIR/airflow_backup_${TIMESTAMP}.sql.gz"
    
    echo -e "${GREEN}Creating PostgreSQL backup...${NC}"
    log_message "Starting backup of database: $DB_NAME"
    
    # Create backup using pg_dump
    if docker exec -t "$CONTAINER_NAME" pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$backup_file"; then
        local file_size=$(du -h "$backup_file" | cut -f1)
        echo -e "${GREEN}✓ Backup created successfully!${NC}"
        log_message "Backup completed: $backup_file (Size: $file_size)"
        echo "  File: $backup_file"
        echo "  Size: $file_size"
        return 0
    else
        echo -e "${RED}✗ Backup failed!${NC}"
        log_message "ERROR: Backup failed for database $DB_NAME"
        return 1
    fi
}

# Function to create a full dump with schema and data separately
create_detailed_backup() {
    echo -e "${YELLOW}Creating detailed backup with separate schema and data files...${NC}"
    
    # Schema only backup
    local schema_file="$BACKUP_DIR/airflow_schema_${TIMESTAMP}.sql"
    if docker exec -t "$CONTAINER_NAME" pg_dump -U "$DB_USER" --schema-only "$DB_NAME" > "$schema_file"; then
        echo -e "${GREEN}✓ Schema backup created${NC}"
        gzip "$schema_file"
    fi
    
    # Data only backup
    local data_file="$BACKUP_DIR/airflow_data_${TIMESTAMP}.sql"
    if docker exec -t "$CONTAINER_NAME" pg_dump -U "$DB_USER" --data-only "$DB_NAME" > "$data_file"; then
        echo -e "${GREEN}✓ Data backup created${NC}"
        gzip "$data_file"
    fi
}

# Function to backup specific tables
backup_tables() {
    local tables=("employees" "products" "sales_transactions" "support_tickets" "web_analytics" "healthcare_records")
    
    echo -e "${YELLOW}Creating individual table backups...${NC}"
    
    for table in "${tables[@]}"; do
        local table_file="$BACKUP_DIR/${table}_backup_${TIMESTAMP}.sql.gz"
        if docker exec -t "$CONTAINER_NAME" pg_dump -U "$DB_USER" -t "$table" "$DB_NAME" | gzip > "$table_file"; then
            echo -e "${GREEN}✓ Backed up table: $table${NC}"
        else
            echo -e "${RED}✗ Failed to backup table: $table${NC}"
        fi
    done
}

# Function to cleanup old backups
cleanup_old_backups() {
    echo -e "${YELLOW}Cleaning up old backups (keeping last $KEEP_DAYS days)...${NC}"
    
    local deleted_count=0
    while IFS= read -r -d '' file; do
        rm -f "$file"
        ((deleted_count++))
        log_message "Deleted old backup: $(basename "$file")"
    done < <(find "$BACKUP_DIR" -name "*.sql.gz" -type f -mtime +$KEEP_DAYS -print0)
    
    if [ $deleted_count -gt 0 ]; then
        echo -e "${GREEN}✓ Deleted $deleted_count old backup(s)${NC}"
    else
        echo "  No old backups to delete"
    fi
}

# Function to verify backup
verify_backup() {
    local latest_backup=$(ls -t "$BACKUP_DIR"/airflow_backup_*.sql.gz 2>/dev/null | head -1)
    
    if [ -z "$latest_backup" ]; then
        echo -e "${RED}No backup file found to verify${NC}"
        return 1
    fi
    
    echo -e "${YELLOW}Verifying latest backup...${NC}"
    
    # Check if file is valid gzip
    if gzip -t "$latest_backup" 2>/dev/null; then
        echo -e "${GREEN}✓ Backup file is valid${NC}"
        
        # Show backup info
        echo "  Backup details:"
        echo "  - File: $(basename "$latest_backup")"
        echo "  - Size: $(du -h "$latest_backup" | cut -f1)"
        echo "  - Created: $(stat -c %y "$latest_backup" 2>/dev/null || stat -f %Sm "$latest_backup" 2>/dev/null)"
        return 0
    else
        echo -e "${RED}✗ Backup file is corrupted${NC}"
        return 1
    fi
}

# Function to list all backups
list_backups() {
    echo -e "${YELLOW}Available backups:${NC}"
    echo "=================="
    
    if ls "$BACKUP_DIR"/*.sql.gz >/dev/null 2>&1; then
        ls -lh "$BACKUP_DIR"/*.sql.gz | awk '{print $9, "-", $5}' | sed 's|.*/||'
    else
        echo "No backups found"
    fi
}

# Function to restore from backup
restore_backup() {
    local backup_file="$1"
    
    if [ -z "$backup_file" ]; then
        echo -e "${RED}Error: No backup file specified${NC}"
        echo "Usage: $0 restore <backup_file>"
        return 1
    fi
    
    if [ ! -f "$backup_file" ]; then
        echo -e "${RED}Error: Backup file not found: $backup_file${NC}"
        return 1
    fi
    
    echo -e "${YELLOW}WARNING: This will restore the database from backup.${NC}"
    echo -e "${YELLOW}Current data will be overwritten!${NC}"
    read -p "Are you sure you want to continue? (yes/no): " confirm
    
    if [ "$confirm" != "yes" ]; then
        echo "Restore cancelled"
        return 0
    fi
    
    echo -e "${GREEN}Restoring from backup: $(basename "$backup_file")${NC}"
    
    # Drop and recreate database
    docker exec -t "$CONTAINER_NAME" psql -U "$DB_USER" -c "DROP DATABASE IF EXISTS ${DB_NAME}_temp;"
    docker exec -t "$CONTAINER_NAME" psql -U "$DB_USER" -c "CREATE DATABASE ${DB_NAME}_temp;"
    
    # Restore backup
    if zcat "$backup_file" | docker exec -i "$CONTAINER_NAME" psql -U "$DB_USER" "${DB_NAME}_temp"; then
        echo -e "${GREEN}✓ Backup restored successfully to ${DB_NAME}_temp${NC}"
        echo "To complete the restore, you need to:"
        echo "1. Stop Airflow services"
        echo "2. Rename ${DB_NAME}_temp to ${DB_NAME}"
        echo "3. Restart Airflow services"
    else
        echo -e "${RED}✗ Restore failed${NC}"
        return 1
    fi
}

# Function to show database statistics
show_stats() {
    echo -e "${YELLOW}Database Statistics:${NC}"
    echo "==================="
    
    # Database size
    docker exec -t "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            pg_database.datname as database,
            pg_size_pretty(pg_database_size(pg_database.datname)) as size
        FROM pg_database 
        WHERE datname = '$DB_NAME';"
    
    # Table sizes
    echo -e "\n${YELLOW}Table Sizes:${NC}"
    docker exec -t "$CONTAINER_NAME" psql -U "$DB_USER" -d "$DB_NAME" -c "
        SELECT 
            tablename,
            pg_size_pretty(pg_total_relation_size(schemaname||'.'||tablename)) as size,
            n_live_tup as row_count
        FROM pg_stat_user_tables 
        ORDER BY pg_total_relation_size(schemaname||'.'||tablename) DESC
        LIMIT 10;"
}

# Main script execution
main() {
    # Create backup directory if it doesn't exist
    mkdir -p "$BACKUP_DIR"
    
    # Parse command line arguments
    case "$1" in
        "full")
            check_container
            create_backup
            create_detailed_backup
            cleanup_old_backups
            verify_backup
            ;;
        "tables")
            check_container
            backup_tables
            cleanup_old_backups
            ;;
        "list")
            list_backups
            ;;
        "stats")
            check_container
            show_stats
            ;;
        "restore")
            check_container
            restore_backup "$2"
            ;;
        "verify")
            verify_backup
            ;;
        *)
            # Default action - create standard backup
            check_container
            create_backup
            cleanup_old_backups
            verify_backup
            ;;
    esac
}

# Show usage if help is requested
if [ "$1" == "-h" ] || [ "$1" == "--help" ]; then
    echo "PostgreSQL Backup Script for Apache Airflow"
    echo "=========================================="
    echo "Usage: $0 [command] [options]"
    echo ""
    echo "Commands:"
    echo "  (no command)    - Create standard backup"
    echo "  full           - Create full backup with separate schema and data"
    echo "  tables         - Backup individual tables"
    echo "  list           - List all available backups"
    echo "  stats          - Show database statistics"
    echo "  restore <file> - Restore from a backup file"
    echo "  verify         - Verify the latest backup"
    echo "  -h, --help     - Show this help message"
    echo ""
    echo "Examples:"
    echo "  $0                    # Create standard backup"
    echo "  $0 full              # Create full detailed backup"
    echo "  $0 restore backups/airflow_backup_20240315_120000.sql.gz"
    echo ""
    echo "Configuration:"
    echo "  Container: $CONTAINER_NAME"
    echo "  Database: $DB_NAME"
    echo "  Backup dir: $BACKUP_DIR"
    echo "  Keep days: $KEEP_DAYS"
    exit 0
fi

# Run main function
main "$@"

# Exit with success
exit 0.

I have other files that I used to debug the code like this:
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

and that was the final one.

Now I have this logs: (3.11.0) derekdegbedzui@MacBookPro airflow_csv_automation_part_two % docker-compose down
docker-compose up -d
WARN[0000] /Users/derekdegbedzui/Desktop/airflow_csv_automation_part_two/docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
[+] Running 6/6
 ✔ Container airflow-csv-postgres-airflow-scheduler-1  Removed                                                                                                                                   3.1s 
 ✔ Container airflow-csv-postgres-airflow-init-1       Removed                                                                                                                                   0.3s 
 ✔ Container airflow-csv-postgres-airflow-webserver-1  Removed                                                                                                                                   6.4s 
 ✔ Container airflow-csv-postgres-pgadmin-1            Removed                                                                                                                                   2.5s 
 ✔ Container airflow-csv-postgres-postgres-1           Removed                                                                                                                                   0.3s 
 ✔ Network airflow-csv-postgres_default                Removed                                                                                                                                   0.2s 
WARN[0000] /Users/derekdegbedzui/Desktop/airflow_csv_automation_part_two/docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
[+] Running 6/6
 ✔ Network airflow-csv-postgres_default                Created                                                                                                                                   0.1s 
 ✔ Container airflow-csv-postgres-postgres-1           Healthy                                                                                                                                   6.5s 
 ✔ Container airflow-csv-postgres-airflow-webserver-1  Started                                                                                                                                   6.7s 
 ✔ Container airflow-csv-postgres-airflow-scheduler-1  Started                                                                                                                                   6.7s 
 ✔ Container airflow-csv-postgres-airflow-init-1       Started                                                                                                                                   6.6s 
 ✔ Container airflow-csv-postgres-pgadmin-1            Started                                                                                                                                   1.2s 
(3.11.0) derekdegbedzui@MacBookPro airflow_csv_automation_part_two % docker-compose logs -f
WARN[0000] /Users/derekdegbedzui/Desktop/airflow_csv_automation_part_two/docker-compose.yml: the attribute `version` is obsolete, it will be ignored, please remove it to avoid potential confusion 
airflow-webserver-1  | 
airflow-webserver-1  | 
airflow-webserver-1  | !!!!!  Installing additional requirements: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres' !!!!!!!!!!!!
airflow-webserver-1  | 
airflow-webserver-1  | WARNING: This is a development/test feature only. NEVER use it in production!
airflow-webserver-1  |          Instead, build a custom image as described in
airflow-webserver-1  | 
airflow-webserver-1  |          https://airflow.apache.org/docs/docker-stack/build.html
airflow-webserver-1  | 
airflow-webserver-1  |          Adding requirements at container startup is fragile and is done every time
airflow-webserver-1  |          the container starts, so it is only useful for testing and trying out
airflow-webserver-1  |          of adding dependencies.
airflow-webserver-1  | 
airflow-webserver-1  | Requirement already satisfied: pandas in /home/airflow/.local/lib/python3.9/site-packages (2.1.4)
airflow-webserver-1  | Requirement already satisfied: numpy in /home/airflow/.local/lib/python3.9/site-packages (1.26.4)
airflow-webserver-1  | Requirement already satisfied: psycopg2-binary in /home/airflow/.local/lib/python3.9/site-packages (2.9.9)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-postgres in /home/airflow/.local/lib/python3.9/site-packages (5.10.2)
airflow-scheduler-1  | 
airflow-webserver-1  | Requirement already satisfied: python-dateutil>=2.8.2 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2.9.0.post0)
airflow-webserver-1  | Requirement already satisfied: pytz>=2020.1 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2024.1)
airflow-webserver-1  | Requirement already satisfied: tzdata>=2022.1 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2024.1)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-common-sql>=1.3.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-postgres) (1.12.0)
airflow-webserver-1  | Requirement already satisfied: apache-airflow>=2.6.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-postgres) (2.9.1)
airflow-webserver-1  | Requirement already satisfied: alembic<2.0,>=1.13.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.13.1)
airflow-webserver-1  | Requirement already satisfied: argcomplete>=1.10 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.3.0)
airflow-webserver-1  | Requirement already satisfied: asgiref in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.8.1)
airflow-webserver-1  | Requirement already satisfied: attrs>=22.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (23.2.0)
airflow-webserver-1  | Requirement already satisfied: blinker>=1.6.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.8.1)
airflow-webserver-1  | Requirement already satisfied: colorlog<5.0,>=4.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.8.0)
airflow-webserver-1  | Requirement already satisfied: configupdater>=3.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.2)
airflow-webserver-1  | Requirement already satisfied: connexion<3.0,>=2.10.0 in /home/airflow/.local/lib/python3.9/site-packages (from connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.2)
airflow-webserver-1  | Requirement already satisfied: cron-descriptor>=1.2.24 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.3)
airflow-scheduler-1  | !!!!!  Installing additional requirements: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres' !!!!!!!!!!!!
airflow-scheduler-1  | 
airflow-init-1       | 
airflow-scheduler-1  | WARNING: This is a development/test feature only. NEVER use it in production!
airflow-scheduler-1  |          Instead, build a custom image as described in
airflow-scheduler-1  | 
airflow-scheduler-1  |          https://airflow.apache.org/docs/docker-stack/build.html
airflow-scheduler-1  | 
airflow-scheduler-1  |          Adding requirements at container startup is fragile and is done every time
airflow-scheduler-1  |          the container starts, so it is only useful for testing and trying out
airflow-scheduler-1  |          of adding dependencies.
airflow-scheduler-1  | 
airflow-scheduler-1  | 
airflow-scheduler-1  | Requirement already satisfied: pandas in /home/airflow/.local/lib/python3.9/site-packages (2.1.4)
airflow-scheduler-1  | Requirement already satisfied: numpy in /home/airflow/.local/lib/python3.9/site-packages (1.26.4)
airflow-scheduler-1  | Requirement already satisfied: psycopg2-binary in /home/airflow/.local/lib/python3.9/site-packages (2.9.9)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-postgres in /home/airflow/.local/lib/python3.9/site-packages (5.10.2)
airflow-scheduler-1  | Requirement already satisfied: python-dateutil>=2.8.2 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2.9.0.post0)
airflow-scheduler-1  | Requirement already satisfied: pytz>=2020.1 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2024.1)
airflow-scheduler-1  | Requirement already satisfied: tzdata>=2022.1 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2024.1)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-common-sql>=1.3.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-postgres) (1.12.0)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow>=2.6.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-postgres) (2.9.1)
airflow-scheduler-1  | Requirement already satisfied: alembic<2.0,>=1.13.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.13.1)
airflow-scheduler-1  | Requirement already satisfied: argcomplete>=1.10 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.3.0)
airflow-scheduler-1  | Requirement already satisfied: asgiref in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.8.1)
airflow-init-1       | 
airflow-init-1       | !!!!!  Installing additional requirements: 'pandas numpy psycopg2-binary apache-airflow-providers-postgres' !!!!!!!!!!!!
postgres-1           | 
postgres-1           | PostgreSQL Database directory appears to contain a database; Skipping initialization
postgres-1           | 
postgres-1           | 2025-06-08 15:16:06.046 UTC [1] LOG:  starting PostgreSQL 13.21 (Debian 13.21-1.pgdg120+1) on x86_64-pc-linux-gnu, compiled by gcc (Debian 12.2.0-14) 12.2.0, 64-bit
postgres-1           | 2025-06-08 15:16:06.046 UTC [1] LOG:  listening on IPv4 address "0.0.0.0", port 5432
postgres-1           | 2025-06-08 15:16:06.047 UTC [1] LOG:  listening on IPv6 address "::", port 5432
postgres-1           | 2025-06-08 15:16:06.052 UTC [1] LOG:  listening on Unix socket "/var/run/postgresql/.s.PGSQL.5432"
postgres-1           | 2025-06-08 15:16:06.063 UTC [28] LOG:  database system was shut down at 2025-06-08 15:16:04 UTC
postgres-1           | 2025-06-08 15:16:06.074 UTC [1] LOG:  database system is ready to accept connections
airflow-webserver-1  | Requirement already satisfied: croniter>=2.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.5)
airflow-webserver-1  | Requirement already satisfied: cryptography>=39.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (41.0.7)
airflow-webserver-1  | Requirement already satisfied: deprecated>=1.2.13 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.14)
airflow-webserver-1  | Requirement already satisfied: dill>=0.2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.3.1.1)
airflow-webserver-1  | Requirement already satisfied: flask-caching>=1.5.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.0)
airflow-webserver-1  | Requirement already satisfied: flask-session<0.6,>=0.4.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.5.0)
airflow-init-1       | 
airflow-init-1       | WARNING: This is a development/test feature only. NEVER use it in production!
airflow-init-1       |          Instead, build a custom image as described in
airflow-init-1       | 
airflow-init-1       |          https://airflow.apache.org/docs/docker-stack/build.html
airflow-init-1       | 
airflow-init-1       |          Adding requirements at container startup is fragile and is done every time
airflow-init-1       |          the container starts, so it is only useful for testing and trying out
airflow-init-1       |          of adding dependencies.
airflow-init-1       | 
airflow-init-1       | Requirement already satisfied: pandas in /home/airflow/.local/lib/python3.9/site-packages (2.1.4)
airflow-init-1       | Requirement already satisfied: numpy in /home/airflow/.local/lib/python3.9/site-packages (1.26.4)
airflow-init-1       | Requirement already satisfied: psycopg2-binary in /home/airflow/.local/lib/python3.9/site-packages (2.9.9)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-postgres in /home/airflow/.local/lib/python3.9/site-packages (5.10.2)
airflow-init-1       | Requirement already satisfied: python-dateutil>=2.8.2 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2.9.0.post0)
airflow-init-1       | Requirement already satisfied: pytz>=2020.1 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2024.1)
airflow-init-1       | Requirement already satisfied: tzdata>=2022.1 in /home/airflow/.local/lib/python3.9/site-packages (from pandas) (2024.1)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-common-sql>=1.3.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-postgres) (1.12.0)
airflow-init-1       | Requirement already satisfied: apache-airflow>=2.6.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-postgres) (2.9.1)
airflow-init-1       | Requirement already satisfied: alembic<2.0,>=1.13.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.13.1)
airflow-init-1       | Requirement already satisfied: argcomplete>=1.10 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.3.0)
airflow-init-1       | Requirement already satisfied: asgiref in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.8.1)
airflow-init-1       | Requirement already satisfied: attrs>=22.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (23.2.0)
airflow-init-1       | Requirement already satisfied: blinker>=1.6.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.8.1)
airflow-init-1       | Requirement already satisfied: colorlog<5.0,>=4.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.8.0)
airflow-init-1       | Requirement already satisfied: configupdater>=3.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.2)
airflow-init-1       | Requirement already satisfied: connexion<3.0,>=2.10.0 in /home/airflow/.local/lib/python3.9/site-packages (from connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.2)
airflow-init-1       | Requirement already satisfied: cron-descriptor>=1.2.24 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.3)
airflow-init-1       | Requirement already satisfied: croniter>=2.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.5)
airflow-init-1       | Requirement already satisfied: cryptography>=39.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (41.0.7)
airflow-webserver-1  | Requirement already satisfied: flask-wtf>=0.15 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.1)
airflow-webserver-1  | Requirement already satisfied: flask<2.3,>=2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.5)
airflow-webserver-1  | Requirement already satisfied: fsspec>=2023.10.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2023.12.2)
airflow-webserver-1  | Requirement already satisfied: google-re2>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.1.20240501)
airflow-webserver-1  | Requirement already satisfied: gunicorn>=20.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (22.0.0)
airflow-webserver-1  | Requirement already satisfied: httpx in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.23.3)
airflow-webserver-1  | Requirement already satisfied: importlib_metadata>=6.5 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.11.0)
airflow-webserver-1  | Requirement already satisfied: itsdangerous>=2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.0)
airflow-webserver-1  | Requirement already satisfied: jinja2>=3.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.3)
airflow-webserver-1  | Requirement already satisfied: jsonschema>=4.18.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.22.0)
airflow-webserver-1  | Requirement already satisfied: lazy-object-proxy in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.10.0)
airflow-webserver-1  | Requirement already satisfied: linkify-it-py>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.3)
airflow-webserver-1  | Requirement already satisfied: lockfile>=0.12.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.12.2)
airflow-webserver-1  | Requirement already satisfied: markdown-it-py>=2.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.0)
airflow-webserver-1  | Requirement already satisfied: markupsafe>=1.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.1.5)
airflow-webserver-1  | Requirement already satisfied: marshmallow-oneofschema>=2.0.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.1)
airflow-webserver-1  | Requirement already satisfied: mdit-py-plugins>=0.3.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.0)
airflow-webserver-1  | Requirement already satisfied: methodtools>=0.4.7 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.7)
airflow-webserver-1  | Requirement already satisfied: opentelemetry-api>=1.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-webserver-1  | Requirement already satisfied: opentelemetry-exporter-otlp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-webserver-1  | Requirement already satisfied: packaging>=14.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (24.0)
airflow-webserver-1  | Requirement already satisfied: pathspec>=0.9.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.12.1)
airflow-webserver-1  | Requirement already satisfied: pendulum<4.0,>=2.1.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.0)
airflow-webserver-1  | Requirement already satisfied: pluggy>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.5.0)
airflow-webserver-1  | Requirement already satisfied: psutil>=4.2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (5.9.8)
airflow-webserver-1  | Requirement already satisfied: pygments>=2.0.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.17.2)
airflow-webserver-1  | Requirement already satisfied: pyjwt>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.8.0)
airflow-webserver-1  | Requirement already satisfied: python-daemon>=3.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.1)
airflow-webserver-1  | Requirement already satisfied: python-nvd3>=0.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16.0)
airflow-scheduler-1  | Requirement already satisfied: attrs>=22.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (23.2.0)
airflow-init-1       | Requirement already satisfied: deprecated>=1.2.13 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.14)
airflow-scheduler-1  | Requirement already satisfied: blinker>=1.6.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.8.1)
airflow-scheduler-1  | Requirement already satisfied: colorlog<5.0,>=4.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.8.0)
airflow-scheduler-1  | Requirement already satisfied: configupdater>=3.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.2)
airflow-init-1       | Requirement already satisfied: dill>=0.2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.3.1.1)
airflow-init-1       | Requirement already satisfied: flask-caching>=1.5.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.0)
airflow-scheduler-1  | Requirement already satisfied: connexion<3.0,>=2.10.0 in /home/airflow/.local/lib/python3.9/site-packages (from connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.2)
airflow-scheduler-1  | Requirement already satisfied: cron-descriptor>=1.2.24 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.3)
airflow-scheduler-1  | Requirement already satisfied: croniter>=2.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.5)
airflow-scheduler-1  | Requirement already satisfied: cryptography>=39.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (41.0.7)
airflow-scheduler-1  | Requirement already satisfied: deprecated>=1.2.13 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.14)
airflow-scheduler-1  | Requirement already satisfied: dill>=0.2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.3.1.1)
airflow-scheduler-1  | Requirement already satisfied: flask-caching>=1.5.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.0)
airflow-scheduler-1  | Requirement already satisfied: flask-session<0.6,>=0.4.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.5.0)
airflow-scheduler-1  | Requirement already satisfied: flask-wtf>=0.15 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.1)
airflow-scheduler-1  | Requirement already satisfied: flask<2.3,>=2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.5)
airflow-scheduler-1  | Requirement already satisfied: fsspec>=2023.10.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2023.12.2)
airflow-scheduler-1  | Requirement already satisfied: google-re2>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.1.20240501)
airflow-scheduler-1  | Requirement already satisfied: gunicorn>=20.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (22.0.0)
airflow-scheduler-1  | Requirement already satisfied: httpx in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.23.3)
airflow-scheduler-1  | Requirement already satisfied: importlib_metadata>=6.5 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.11.0)
airflow-webserver-1  | Requirement already satisfied: python-slugify>=5.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.0.4)
airflow-webserver-1  | Requirement already satisfied: requests<3,>=2.27.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.31.0)
airflow-webserver-1  | Requirement already satisfied: rfc3339-validator>=0.1.4 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.1.4)
airflow-init-1       | Requirement already satisfied: flask-session<0.6,>=0.4.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.5.0)
airflow-webserver-1  | Requirement already satisfied: rich-argparse>=1.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.0)
airflow-webserver-1  | Requirement already satisfied: rich>=12.4.4 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (13.7.1)
airflow-webserver-1  | Requirement already satisfied: setproctitle>=1.1.8 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.3)
airflow-webserver-1  | Requirement already satisfied: sqlalchemy<2.0,>=1.4.36 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.52)
pgadmin-1            | email config is {'CHECK_EMAIL_DELIVERABILITY': False, 'ALLOW_SPECIAL_EMAIL_DOMAINS': [], 'GLOBALLY_DELIVERABLE': True}
pgadmin-1            | /venv/lib/python3.12/site-packages/passlib/pwd.py:16: UserWarning: pkg_resources is deprecated as an API. See https://setuptools.pypa.io/en/latest/pkg_resources.html. The pkg_resources package is slated for removal as early as 2025-11-30. Refrain from using this package or pin to Setuptools<81.
pgadmin-1            |   import pkg_resources
pgadmin-1            | NOTE: Configuring authentication for SERVER mode.
pgadmin-1            | 
airflow-scheduler-1  | Requirement already satisfied: itsdangerous>=2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.0)
airflow-scheduler-1  | Requirement already satisfied: jinja2>=3.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.3)
pgadmin-1            | pgAdmin 4 - Application Initialisation
pgadmin-1            | ======================================
airflow-scheduler-1  | Requirement already satisfied: jsonschema>=4.18.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.22.0)
airflow-init-1       | Requirement already satisfied: flask-wtf>=0.15 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.1)
airflow-init-1       | Requirement already satisfied: flask<2.3,>=2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.5)
airflow-init-1       | Requirement already satisfied: fsspec>=2023.10.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2023.12.2)
airflow-init-1       | Requirement already satisfied: google-re2>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.1.20240501)
airflow-init-1       | Requirement already satisfied: gunicorn>=20.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (22.0.0)
airflow-scheduler-1  | Requirement already satisfied: lazy-object-proxy in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.10.0)
airflow-scheduler-1  | Requirement already satisfied: linkify-it-py>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.3)
airflow-scheduler-1  | Requirement already satisfied: lockfile>=0.12.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.12.2)
airflow-scheduler-1  | Requirement already satisfied: markdown-it-py>=2.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.0)
airflow-scheduler-1  | Requirement already satisfied: markupsafe>=1.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.1.5)
airflow-scheduler-1  | Requirement already satisfied: marshmallow-oneofschema>=2.0.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.1)
airflow-scheduler-1  | Requirement already satisfied: mdit-py-plugins>=0.3.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.0)
airflow-webserver-1  | Requirement already satisfied: sqlalchemy-jsonfield>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.2)
airflow-scheduler-1  | Requirement already satisfied: methodtools>=0.4.7 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.7)
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-api>=1.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-webserver-1  | Requirement already satisfied: tabulate>=0.7.5 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.9.0)
airflow-webserver-1  | Requirement already satisfied: tenacity!=8.2.0,>=6.2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.2.3)
pgadmin-1            | 
pgadmin-1            | postfix/postlog: starting the Postfix mail system
pgadmin-1            | /venv/lib/python3.12/site-packages/passlib/pwd.py:16: UserWarning: pkg_resources is deprecated as an API. See https://setuptools.pypa.io/en/latest/pkg_resources.html. The pkg_resources package is slated for removal as early as 2025-11-30. Refrain from using this package or pin to Setuptools<81.
pgadmin-1            |   import pkg_resources
pgadmin-1            | [2025-06-08 15:16:35 +0000] [1] [INFO] Starting gunicorn 23.0.0
pgadmin-1            | [2025-06-08 15:16:35 +0000] [1] [INFO] Listening at: http://[::]:80 (1)
pgadmin-1            | [2025-06-08 15:16:35 +0000] [1] [INFO] Using worker: gthread
pgadmin-1            | [2025-06-08 15:16:35 +0000] [118] [INFO] Booting worker with pid: 118
pgadmin-1            | /venv/lib/python3.12/site-packages/passlib/pwd.py:16: UserWarning: pkg_resources is deprecated as an API. See https://setuptools.pypa.io/en/latest/pkg_resources.html. The pkg_resources package is slated for removal as early as 2025-11-30. Refrain from using this package or pin to Setuptools<81.
pgadmin-1            |   import pkg_resources
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-exporter-otlp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-scheduler-1  | Requirement already satisfied: packaging>=14.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (24.0)
airflow-scheduler-1  | Requirement already satisfied: pathspec>=0.9.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.12.1)
airflow-webserver-1  | Requirement already satisfied: termcolor>=1.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.4.0)
airflow-webserver-1  | Requirement already satisfied: unicodecsv>=0.14.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.14.1)
airflow-webserver-1  | Requirement already satisfied: universal-pathlib>=0.2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.2.2)
airflow-scheduler-1  | Requirement already satisfied: pendulum<4.0,>=2.1.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.0)
airflow-scheduler-1  | Requirement already satisfied: pluggy>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.5.0)
airflow-webserver-1  | Requirement already satisfied: werkzeug<3,>=2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.3)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-common-io in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-fab>=1.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.4)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-ftp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.8.0)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-http in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.10.1)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-imap in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.5.0)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-smtp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.6.1)
airflow-scheduler-1  | Requirement already satisfied: psutil>=4.2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (5.9.8)
airflow-scheduler-1  | Requirement already satisfied: pygments>=2.0.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.17.2)
airflow-scheduler-1  | Requirement already satisfied: pyjwt>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.8.0)
airflow-scheduler-1  | Requirement already satisfied: python-daemon>=3.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.1)
airflow-scheduler-1  | Requirement already satisfied: python-nvd3>=0.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16.0)
airflow-scheduler-1  | Requirement already satisfied: python-slugify>=5.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.0.4)
airflow-scheduler-1  | Requirement already satisfied: requests<3,>=2.27.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.31.0)
airflow-scheduler-1  | Requirement already satisfied: rfc3339-validator>=0.1.4 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.1.4)
airflow-webserver-1  | Requirement already satisfied: apache-airflow-providers-sqlite in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.7.1)
airflow-webserver-1  | Requirement already satisfied: more-itertools>=9.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-common-sql>=1.3.1->apache-airflow-providers-postgres) (10.2.0)
airflow-webserver-1  | Requirement already satisfied: sqlparse>=0.4.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-common-sql>=1.3.1->apache-airflow-providers-postgres) (0.5.0)
airflow-scheduler-1  | Requirement already satisfied: rich-argparse>=1.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.0)
airflow-scheduler-1  | Requirement already satisfied: rich>=12.4.4 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (13.7.1)
airflow-scheduler-1  | Requirement already satisfied: setproctitle>=1.1.8 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.3)
airflow-scheduler-1  | Requirement already satisfied: sqlalchemy<2.0,>=1.4.36 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.52)
airflow-scheduler-1  | Requirement already satisfied: sqlalchemy-jsonfield>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.2)
airflow-scheduler-1  | Requirement already satisfied: tabulate>=0.7.5 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.9.0)
airflow-webserver-1  | Requirement already satisfied: six>=1.5 in /home/airflow/.local/lib/python3.9/site-packages (from python-dateutil>=2.8.2->pandas) (1.16.0)
airflow-webserver-1  | Requirement already satisfied: Mako in /home/airflow/.local/lib/python3.9/site-packages (from alembic<2.0,>=1.13.1->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.3)
airflow-webserver-1  | Requirement already satisfied: typing-extensions>=4 in /home/airflow/.local/lib/python3.9/site-packages (from alembic<2.0,>=1.13.1->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.11.0)
airflow-webserver-1  | Requirement already satisfied: flask-appbuilder==4.4.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.4.1)
airflow-webserver-1  | Requirement already satisfied: flask-login>=0.6.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.6.3)
airflow-webserver-1  | Requirement already satisfied: apispec<7,>=6.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apispec[yaml]<7,>=6.0.0->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.6.1)
airflow-webserver-1  | Requirement already satisfied: colorama<1,>=0.3.9 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.6)
airflow-webserver-1  | Requirement already satisfied: click<9,>=8 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.1.7)
airflow-webserver-1  | Requirement already satisfied: email-validator>=1.0.5 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.1.1)
airflow-webserver-1  | Requirement already satisfied: Flask-Babel<3,>=1 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.0)
airflow-webserver-1  | Requirement already satisfied: Flask-Limiter<4,>3 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.6.0)
airflow-webserver-1  | Requirement already satisfied: Flask-SQLAlchemy<3,>=2.4 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.5.1)
airflow-webserver-1  | Requirement already satisfied: Flask-JWT-Extended<5.0.0,>=4.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.6.0)
airflow-webserver-1  | Requirement already satisfied: marshmallow<4,>=3.18.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.21.2)
airflow-webserver-1  | Requirement already satisfied: marshmallow-sqlalchemy<0.29.0,>=0.22.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.28.2)
airflow-webserver-1  | Requirement already satisfied: prison<1.0.0,>=0.2.1 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.2.1)
airflow-webserver-1  | Requirement already satisfied: sqlalchemy-utils<1,>=0.32.21 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.41.2)
airflow-webserver-1  | Requirement already satisfied: WTForms<4 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.2)
airflow-webserver-1  | Requirement already satisfied: clickclick<21,>=1.2 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (20.10.2)
airflow-webserver-1  | Requirement already satisfied: PyYAML<7,>=5.1 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.0.1)
airflow-webserver-1  | Requirement already satisfied: inflection<0.6,>=0.3.1 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.5.1)
airflow-webserver-1  | Requirement already satisfied: cffi>=1.12 in /home/airflow/.local/lib/python3.9/site-packages (from cryptography>=39.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.16.0)
airflow-webserver-1  | Requirement already satisfied: wrapt<2,>=1.10 in /home/airflow/.local/lib/python3.9/site-packages (from deprecated>=1.2.13->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.16.0)
airflow-webserver-1  | Requirement already satisfied: cachelib<0.10.0,>=0.9.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-caching>=1.5.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.9.0)
airflow-webserver-1  | Requirement already satisfied: zipp>=0.5 in /home/airflow/.local/lib/python3.9/site-packages (from importlib_metadata>=6.5->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.18.1)
airflow-webserver-1  | Requirement already satisfied: jsonschema-specifications>=2023.03.6 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2023.12.1)
airflow-webserver-1  | Requirement already satisfied: referencing>=0.28.4 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.31.1)
airflow-webserver-1  | Requirement already satisfied: rpds-py>=0.7.1 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.18.0)
airflow-webserver-1  | Requirement already satisfied: uc-micro-py in /home/airflow/.local/lib/python3.9/site-packages (from linkify-it-py>=2.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.3)
airflow-scheduler-1  | Requirement already satisfied: tenacity!=8.2.0,>=6.2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.2.3)
airflow-webserver-1  | Requirement already satisfied: mdurl~=0.1 in /home/airflow/.local/lib/python3.9/site-packages (from markdown-it-py>=2.1.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.1.2)
airflow-webserver-1  | Requirement already satisfied: wirerope>=0.4.7 in /home/airflow/.local/lib/python3.9/site-packages (from methodtools>=0.4.7->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.7)
airflow-webserver-1  | Requirement already satisfied: time-machine>=2.6.0 in /home/airflow/.local/lib/python3.9/site-packages (from pendulum<4.0,>=2.1.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.1)
airflow-init-1       | Requirement already satisfied: httpx in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.23.3)
airflow-init-1       | Requirement already satisfied: importlib_metadata>=6.5 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.11.0)
airflow-init-1       | Requirement already satisfied: itsdangerous>=2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.0)
airflow-init-1       | Requirement already satisfied: jinja2>=3.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.3)
airflow-init-1       | Requirement already satisfied: jsonschema>=4.18.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.22.0)
airflow-init-1       | Requirement already satisfied: lazy-object-proxy in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.10.0)
airflow-init-1       | Requirement already satisfied: linkify-it-py>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.3)
airflow-init-1       | Requirement already satisfied: lockfile>=0.12.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.12.2)
airflow-init-1       | Requirement already satisfied: markdown-it-py>=2.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.0)
airflow-init-1       | Requirement already satisfied: markupsafe>=1.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.1.5)
airflow-init-1       | Requirement already satisfied: marshmallow-oneofschema>=2.0.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.1)
airflow-init-1       | Requirement already satisfied: mdit-py-plugins>=0.3.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.0)
airflow-init-1       | Requirement already satisfied: methodtools>=0.4.7 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.7)
airflow-init-1       | Requirement already satisfied: opentelemetry-api>=1.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-init-1       | Requirement already satisfied: opentelemetry-exporter-otlp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-init-1       | Requirement already satisfied: packaging>=14.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (24.0)
airflow-init-1       | Requirement already satisfied: pathspec>=0.9.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.12.1)
airflow-init-1       | Requirement already satisfied: pendulum<4.0,>=2.1.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.0)
airflow-init-1       | Requirement already satisfied: pluggy>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.5.0)
airflow-init-1       | Requirement already satisfied: psutil>=4.2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (5.9.8)
airflow-init-1       | Requirement already satisfied: pygments>=2.0.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.17.2)
airflow-init-1       | Requirement already satisfied: pyjwt>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.8.0)
airflow-init-1       | Requirement already satisfied: python-daemon>=3.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.1)
airflow-init-1       | Requirement already satisfied: python-nvd3>=0.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16.0)
airflow-init-1       | Requirement already satisfied: python-slugify>=5.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.0.4)
airflow-init-1       | Requirement already satisfied: requests<3,>=2.27.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.31.0)
airflow-init-1       | Requirement already satisfied: rfc3339-validator>=0.1.4 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.1.4)
airflow-init-1       | Requirement already satisfied: rich-argparse>=1.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.0)
airflow-init-1       | Requirement already satisfied: rich>=12.4.4 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (13.7.1)
airflow-init-1       | Requirement already satisfied: setproctitle>=1.1.8 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.3)
airflow-init-1       | Requirement already satisfied: sqlalchemy<2.0,>=1.4.36 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.52)
airflow-init-1       | Requirement already satisfied: sqlalchemy-jsonfield>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.2)
airflow-init-1       | Requirement already satisfied: tabulate>=0.7.5 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.9.0)
airflow-init-1       | Requirement already satisfied: tenacity!=8.2.0,>=6.2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.2.3)
airflow-init-1       | Requirement already satisfied: termcolor>=1.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.4.0)
airflow-scheduler-1  | Requirement already satisfied: termcolor>=1.1.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.4.0)
airflow-scheduler-1  | Requirement already satisfied: unicodecsv>=0.14.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.14.1)
airflow-scheduler-1  | Requirement already satisfied: universal-pathlib>=0.2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.2.2)
airflow-scheduler-1  | Requirement already satisfied: werkzeug<3,>=2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.3)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-common-io in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-fab>=1.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.4)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-ftp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.8.0)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-http in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.10.1)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-imap in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.5.0)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-smtp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.6.1)
airflow-scheduler-1  | Requirement already satisfied: apache-airflow-providers-sqlite in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.7.1)
airflow-scheduler-1  | Requirement already satisfied: more-itertools>=9.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-common-sql>=1.3.1->apache-airflow-providers-postgres) (10.2.0)
airflow-scheduler-1  | Requirement already satisfied: sqlparse>=0.4.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-common-sql>=1.3.1->apache-airflow-providers-postgres) (0.5.0)
airflow-scheduler-1  | Requirement already satisfied: six>=1.5 in /home/airflow/.local/lib/python3.9/site-packages (from python-dateutil>=2.8.2->pandas) (1.16.0)
airflow-scheduler-1  | Requirement already satisfied: Mako in /home/airflow/.local/lib/python3.9/site-packages (from alembic<2.0,>=1.13.1->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.3)
airflow-scheduler-1  | Requirement already satisfied: typing-extensions>=4 in /home/airflow/.local/lib/python3.9/site-packages (from alembic<2.0,>=1.13.1->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.11.0)
airflow-scheduler-1  | Requirement already satisfied: flask-appbuilder==4.4.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.4.1)
airflow-scheduler-1  | Requirement already satisfied: flask-login>=0.6.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.6.3)
airflow-scheduler-1  | Requirement already satisfied: apispec<7,>=6.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apispec[yaml]<7,>=6.0.0->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.6.1)
airflow-scheduler-1  | Requirement already satisfied: colorama<1,>=0.3.9 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.6)
airflow-scheduler-1  | Requirement already satisfied: click<9,>=8 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.1.7)
airflow-scheduler-1  | Requirement already satisfied: email-validator>=1.0.5 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.1.1)
airflow-scheduler-1  | Requirement already satisfied: Flask-Babel<3,>=1 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.0)
airflow-scheduler-1  | Requirement already satisfied: Flask-Limiter<4,>3 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.6.0)
airflow-scheduler-1  | Requirement already satisfied: Flask-SQLAlchemy<3,>=2.4 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.5.1)
airflow-scheduler-1  | Requirement already satisfied: Flask-JWT-Extended<5.0.0,>=4.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.6.0)
airflow-scheduler-1  | Requirement already satisfied: marshmallow<4,>=3.18.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.21.2)
airflow-scheduler-1  | Requirement already satisfied: marshmallow-sqlalchemy<0.29.0,>=0.22.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.28.2)
airflow-scheduler-1  | Requirement already satisfied: prison<1.0.0,>=0.2.1 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.2.1)
airflow-init-1       | Requirement already satisfied: unicodecsv>=0.14.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.14.1)
airflow-init-1       | Requirement already satisfied: universal-pathlib>=0.2.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.2.2)
airflow-init-1       | Requirement already satisfied: werkzeug<3,>=2.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.2.3)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-common-io in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-fab>=1.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.4)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-ftp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.8.0)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-http in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.10.1)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-imap in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.5.0)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-smtp in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.6.1)
airflow-init-1       | Requirement already satisfied: apache-airflow-providers-sqlite in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.7.1)
airflow-init-1       | Requirement already satisfied: more-itertools>=9.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-common-sql>=1.3.1->apache-airflow-providers-postgres) (10.2.0)
airflow-init-1       | Requirement already satisfied: sqlparse>=0.4.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-common-sql>=1.3.1->apache-airflow-providers-postgres) (0.5.0)
airflow-init-1       | Requirement already satisfied: six>=1.5 in /home/airflow/.local/lib/python3.9/site-packages (from python-dateutil>=2.8.2->pandas) (1.16.0)
airflow-init-1       | Requirement already satisfied: Mako in /home/airflow/.local/lib/python3.9/site-packages (from alembic<2.0,>=1.13.1->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.3)
airflow-init-1       | Requirement already satisfied: typing-extensions>=4 in /home/airflow/.local/lib/python3.9/site-packages (from alembic<2.0,>=1.13.1->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.11.0)
airflow-init-1       | Requirement already satisfied: flask-appbuilder==4.4.1 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.4.1)
airflow-init-1       | Requirement already satisfied: flask-login>=0.6.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.6.3)
airflow-init-1       | Requirement already satisfied: apispec<7,>=6.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from apispec[yaml]<7,>=6.0.0->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.6.1)
airflow-init-1       | Requirement already satisfied: colorama<1,>=0.3.9 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.6)
airflow-init-1       | Requirement already satisfied: click<9,>=8 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (8.1.7)
airflow-init-1       | Requirement already satisfied: email-validator>=1.0.5 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.1.1)
airflow-init-1       | Requirement already satisfied: Flask-Babel<3,>=1 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.0.0)
airflow-init-1       | Requirement already satisfied: Flask-Limiter<4,>3 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.6.0)
airflow-init-1       | Requirement already satisfied: Flask-SQLAlchemy<3,>=2.4 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.5.1)
airflow-init-1       | Requirement already satisfied: Flask-JWT-Extended<5.0.0,>=4.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.6.0)
airflow-init-1       | Requirement already satisfied: marshmallow<4,>=3.18.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.21.2)
airflow-init-1       | Requirement already satisfied: marshmallow-sqlalchemy<0.29.0,>=0.22.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.28.2)
airflow-scheduler-1  | Requirement already satisfied: sqlalchemy-utils<1,>=0.32.21 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.41.2)
airflow-scheduler-1  | Requirement already satisfied: WTForms<4 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.2)
airflow-scheduler-1  | Requirement already satisfied: clickclick<21,>=1.2 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (20.10.2)
airflow-scheduler-1  | Requirement already satisfied: PyYAML<7,>=5.1 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.0.1)
airflow-scheduler-1  | Requirement already satisfied: inflection<0.6,>=0.3.1 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.5.1)
airflow-scheduler-1  | Requirement already satisfied: cffi>=1.12 in /home/airflow/.local/lib/python3.9/site-packages (from cryptography>=39.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.16.0)
airflow-scheduler-1  | Requirement already satisfied: wrapt<2,>=1.10 in /home/airflow/.local/lib/python3.9/site-packages (from deprecated>=1.2.13->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.16.0)
airflow-scheduler-1  | Requirement already satisfied: cachelib<0.10.0,>=0.9.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-caching>=1.5.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.9.0)
airflow-scheduler-1  | Requirement already satisfied: zipp>=0.5 in /home/airflow/.local/lib/python3.9/site-packages (from importlib_metadata>=6.5->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.18.1)
airflow-scheduler-1  | Requirement already satisfied: jsonschema-specifications>=2023.03.6 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2023.12.1)
airflow-scheduler-1  | Requirement already satisfied: referencing>=0.28.4 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.31.1)
airflow-scheduler-1  | Requirement already satisfied: rpds-py>=0.7.1 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.18.0)
airflow-scheduler-1  | Requirement already satisfied: uc-micro-py in /home/airflow/.local/lib/python3.9/site-packages (from linkify-it-py>=2.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.3)
airflow-scheduler-1  | Requirement already satisfied: mdurl~=0.1 in /home/airflow/.local/lib/python3.9/site-packages (from markdown-it-py>=2.1.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.1.2)
airflow-scheduler-1  | Requirement already satisfied: wirerope>=0.4.7 in /home/airflow/.local/lib/python3.9/site-packages (from methodtools>=0.4.7->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.7)
airflow-scheduler-1  | Requirement already satisfied: time-machine>=2.6.0 in /home/airflow/.local/lib/python3.9/site-packages (from pendulum<4.0,>=2.1.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.1)
airflow-init-1       | Requirement already satisfied: prison<1.0.0,>=0.2.1 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.2.1)
airflow-init-1       | Requirement already satisfied: sqlalchemy-utils<1,>=0.32.21 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.41.2)
airflow-init-1       | Requirement already satisfied: WTForms<4 in /home/airflow/.local/lib/python3.9/site-packages (from flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.1.2)
airflow-webserver-1  | Requirement already satisfied: docutils in /home/airflow/.local/lib/python3.9/site-packages (from python-daemon>=3.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16)
airflow-webserver-1  | Requirement already satisfied: setuptools>=62.4.0 in /home/airflow/.local/lib/python3.9/site-packages (from python-daemon>=3.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (66.1.1)
airflow-webserver-1  | Requirement already satisfied: text-unidecode>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from python-slugify>=5.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3)
airflow-webserver-1  | Requirement already satisfied: charset-normalizer<4,>=2 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.3.2)
airflow-scheduler-1  | Requirement already satisfied: docutils in /home/airflow/.local/lib/python3.9/site-packages (from python-daemon>=3.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16)
airflow-scheduler-1  | Requirement already satisfied: setuptools>=62.4.0 in /home/airflow/.local/lib/python3.9/site-packages (from python-daemon>=3.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (66.1.1)
airflow-scheduler-1  | Requirement already satisfied: text-unidecode>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from python-slugify>=5.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3)
airflow-scheduler-1  | Requirement already satisfied: charset-normalizer<4,>=2 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.3.2)
airflow-init-1       | Requirement already satisfied: clickclick<21,>=1.2 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (20.10.2)
airflow-init-1       | Requirement already satisfied: PyYAML<7,>=5.1 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.0.1)
airflow-init-1       | Requirement already satisfied: inflection<0.6,>=0.3.1 in /home/airflow/.local/lib/python3.9/site-packages (from connexion<3.0,>=2.10.0->connexion[flask]<3.0,>=2.10.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.5.1)
airflow-init-1       | Requirement already satisfied: cffi>=1.12 in /home/airflow/.local/lib/python3.9/site-packages (from cryptography>=39.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.16.0)
airflow-init-1       | Requirement already satisfied: wrapt<2,>=1.10 in /home/airflow/.local/lib/python3.9/site-packages (from deprecated>=1.2.13->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.16.0)
airflow-init-1       | Requirement already satisfied: cachelib<0.10.0,>=0.9.0 in /home/airflow/.local/lib/python3.9/site-packages (from flask-caching>=1.5.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.9.0)
airflow-init-1       | Requirement already satisfied: zipp>=0.5 in /home/airflow/.local/lib/python3.9/site-packages (from importlib_metadata>=6.5->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.18.1)
airflow-init-1       | Requirement already satisfied: jsonschema-specifications>=2023.03.6 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2023.12.1)
airflow-init-1       | Requirement already satisfied: referencing>=0.28.4 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.31.1)
airflow-scheduler-1  | Requirement already satisfied: idna<4,>=2.5 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.7)
airflow-init-1       | Requirement already satisfied: rpds-py>=0.7.1 in /home/airflow/.local/lib/python3.9/site-packages (from jsonschema>=4.18.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.18.0)
airflow-init-1       | Requirement already satisfied: uc-micro-py in /home/airflow/.local/lib/python3.9/site-packages (from linkify-it-py>=2.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.3)
airflow-init-1       | Requirement already satisfied: mdurl~=0.1 in /home/airflow/.local/lib/python3.9/site-packages (from markdown-it-py>=2.1.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.1.2)
airflow-init-1       | Requirement already satisfied: wirerope>=0.4.7 in /home/airflow/.local/lib/python3.9/site-packages (from methodtools>=0.4.7->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.4.7)
airflow-init-1       | Requirement already satisfied: time-machine>=2.6.0 in /home/airflow/.local/lib/python3.9/site-packages (from pendulum<4.0,>=2.1.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.1)
airflow-init-1       | Requirement already satisfied: docutils in /home/airflow/.local/lib/python3.9/site-packages (from python-daemon>=3.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16)
airflow-webserver-1  | Requirement already satisfied: idna<4,>=2.5 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.7)
airflow-init-1       | Requirement already satisfied: setuptools>=62.4.0 in /home/airflow/.local/lib/python3.9/site-packages (from python-daemon>=3.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (66.1.1)
airflow-webserver-1  | Requirement already satisfied: urllib3<3,>=1.21.1 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.26.18)
airflow-scheduler-1  | Requirement already satisfied: urllib3<3,>=1.21.1 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.26.18)
airflow-scheduler-1  | Requirement already satisfied: certifi>=2017.4.17 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2024.2.2)
airflow-scheduler-1  | Requirement already satisfied: greenlet!=0.4.17 in /home/airflow/.local/lib/python3.9/site-packages (from sqlalchemy<2.0,>=1.4.36->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.3)
airflow-scheduler-1  | Requirement already satisfied: aiohttp>=3.9.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.9.5)
airflow-scheduler-1  | Requirement already satisfied: requests_toolbelt in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.0)
airflow-scheduler-1  | Requirement already satisfied: httpcore<0.17.0,>=0.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16.3)
airflow-scheduler-1  | Requirement already satisfied: rfc3986<2,>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from rfc3986[idna2008]<2,>=1.3->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.5.0)
airflow-webserver-1  | Requirement already satisfied: certifi>=2017.4.17 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2024.2.2)
airflow-webserver-1  | Requirement already satisfied: greenlet!=0.4.17 in /home/airflow/.local/lib/python3.9/site-packages (from sqlalchemy<2.0,>=1.4.36->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.3)
airflow-webserver-1  | Requirement already satisfied: aiohttp>=3.9.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.9.5)
airflow-webserver-1  | Requirement already satisfied: requests_toolbelt in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.0)
airflow-webserver-1  | Requirement already satisfied: httpcore<0.17.0,>=0.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16.3)
airflow-webserver-1  | Requirement already satisfied: rfc3986<2,>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from rfc3986[idna2008]<2,>=1.3->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.5.0)
airflow-webserver-1  | Requirement already satisfied: sniffio in /home/airflow/.local/lib/python3.9/site-packages (from httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-scheduler-1  | Requirement already satisfied: sniffio in /home/airflow/.local/lib/python3.9/site-packages (from httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-webserver-1  | Requirement already satisfied: opentelemetry-exporter-otlp-proto-grpc==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-webserver-1  | Requirement already satisfied: opentelemetry-exporter-otlp-proto-http==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-exporter-otlp-proto-grpc==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-exporter-otlp-proto-http==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-scheduler-1  | Requirement already satisfied: googleapis-common-protos~=1.52 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.63.0)
airflow-scheduler-1  | Requirement already satisfied: grpcio<2.0.0,>=1.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.63.0)
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-exporter-otlp-proto-common==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-proto==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-sdk~=1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-scheduler-1  | Requirement already satisfied: protobuf<5.0,>=3.19 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-proto==1.24.0->opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.25.3)
airflow-scheduler-1  | Requirement already satisfied: aiosignal>=1.1.2 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-scheduler-1  | Requirement already satisfied: frozenlist>=1.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.1)
airflow-scheduler-1  | Requirement already satisfied: multidict<7.0,>=4.5 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.0.5)
airflow-scheduler-1  | Requirement already satisfied: yarl<2.0,>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.9.4)
airflow-scheduler-1  | Requirement already satisfied: async-timeout<5.0,>=4.0 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.0.3)
airflow-scheduler-1  | Requirement already satisfied: pycparser in /home/airflow/.local/lib/python3.9/site-packages (from cffi>=1.12->cryptography>=39.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.22)
airflow-scheduler-1  | Requirement already satisfied: h11<0.15,>=0.13 in /home/airflow/.local/lib/python3.9/site-packages (from httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.14.0)
airflow-scheduler-1  | Requirement already satisfied: anyio<5.0,>=3.0 in /home/airflow/.local/lib/python3.9/site-packages (from httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.3.0)
airflow-webserver-1  | Requirement already satisfied: googleapis-common-protos~=1.52 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.63.0)
airflow-scheduler-1  | Requirement already satisfied: exceptiongroup>=1.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from anyio<5.0,>=3.0->httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.1)
airflow-scheduler-1  | Requirement already satisfied: dnspython>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from email-validator>=1.0.5->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.6.1)
airflow-scheduler-1  | Requirement already satisfied: Babel>=2.3 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Babel<3,>=1->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.0)
airflow-scheduler-1  | Requirement already satisfied: limits>=2.8 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.11.0)
airflow-scheduler-1  | Requirement already satisfied: ordered-set<5,>4 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.1.0)
airflow-scheduler-1  | Requirement already satisfied: opentelemetry-semantic-conventions==0.45b0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-sdk~=1.24.0->opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.45b0)
airflow-scheduler-1  | Requirement already satisfied: importlib-resources>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from limits>=2.8->Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.4.0)
airflow-webserver-1  | Requirement already satisfied: grpcio<2.0.0,>=1.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.63.0)
airflow-scheduler-1  | 
airflow-scheduler-1  | [notice] A new release of pip is available: 24.0 -> 25.1.1
airflow-scheduler-1  | [notice] To update, run: pip install --upgrade pip
airflow-scheduler-1  |   ____________       _____________
airflow-scheduler-1  |  ____    |__( )_________  __/__  /________      __
airflow-scheduler-1  | ____  /| |_  /__  ___/_  /_ __  /_  __ \_ | /| / /
airflow-scheduler-1  | ___  ___ |  / _  /   _  __/ _  / / /_/ /_ |/ |/ /
airflow-scheduler-1  |  _/_/  |_/_/  /_/    /_/    /_/  \____/____/|__/
airflow-scheduler-1  | [2025-06-08T15:16:43.878+0000] {task_context_logger.py:63} INFO - Task context logging is enabled
airflow-webserver-1  | Requirement already satisfied: opentelemetry-exporter-otlp-proto-common==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-webserver-1  | Requirement already satisfied: opentelemetry-proto==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-webserver-1  | Requirement already satisfied: opentelemetry-sdk~=1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-webserver-1  | Requirement already satisfied: protobuf<5.0,>=3.19 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-proto==1.24.0->opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.25.3)
airflow-scheduler-1  | [2025-06-08T15:16:43.879+0000] {executor_loader.py:235} INFO - Loaded executor: LocalExecutor
airflow-scheduler-1  | [2025-06-08T15:16:43.977+0000] {scheduler_job_runner.py:796} INFO - Starting the scheduler
airflow-scheduler-1  | [2025-06-08T15:16:43.978+0000] {scheduler_job_runner.py:803} INFO - Processing each file at most -1 times
airflow-scheduler-1  | [2025-06-08 15:16:43 +0000] [38] [INFO] Starting gunicorn 22.0.0
airflow-webserver-1  | Requirement already satisfied: aiosignal>=1.1.2 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-webserver-1  | Requirement already satisfied: frozenlist>=1.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.1)
airflow-webserver-1  | Requirement already satisfied: multidict<7.0,>=4.5 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.0.5)
airflow-webserver-1  | Requirement already satisfied: yarl<2.0,>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.9.4)
airflow-webserver-1  | Requirement already satisfied: async-timeout<5.0,>=4.0 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.0.3)
airflow-init-1       | Requirement already satisfied: text-unidecode>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from python-slugify>=5.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3)
airflow-webserver-1  | Requirement already satisfied: pycparser in /home/airflow/.local/lib/python3.9/site-packages (from cffi>=1.12->cryptography>=39.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.22)
airflow-webserver-1  | Requirement already satisfied: h11<0.15,>=0.13 in /home/airflow/.local/lib/python3.9/site-packages (from httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.14.0)
airflow-webserver-1  | Requirement already satisfied: anyio<5.0,>=3.0 in /home/airflow/.local/lib/python3.9/site-packages (from httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.3.0)
airflow-scheduler-1  | [2025-06-08 15:16:43 +0000] [38] [INFO] Listening at: http://[::]:8793 (38)
airflow-scheduler-1  | [2025-06-08 15:16:43 +0000] [38] [INFO] Using worker: sync
airflow-scheduler-1  | [2025-06-08 15:16:43 +0000] [39] [INFO] Booting worker with pid: 39
airflow-webserver-1  | Requirement already satisfied: exceptiongroup>=1.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from anyio<5.0,>=3.0->httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.1)
airflow-scheduler-1  | [2025-06-08 15:16:44 +0000] [48] [INFO] Booting worker with pid: 48
airflow-scheduler-1  | [2025-06-08T15:16:44.259+0000] {manager.py:170} INFO - Launched DagFileProcessorManager with pid: 157
airflow-scheduler-1  | [2025-06-08T15:16:44.263+0000] {scheduler_job_runner.py:1595} INFO - Adopting or resetting orphaned tasks for active dag runs
airflow-scheduler-1  | [2025-06-08T15:16:44.318+0000] {settings.py:60} INFO - Configured default timezone UTC
airflow-init-1       | Requirement already satisfied: charset-normalizer<4,>=2 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.3.2)
airflow-webserver-1  | Requirement already satisfied: dnspython>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from email-validator>=1.0.5->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.6.1)
airflow-webserver-1  | Requirement already satisfied: Babel>=2.3 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Babel<3,>=1->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.0)
airflow-webserver-1  | Requirement already satisfied: limits>=2.8 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.11.0)
airflow-webserver-1  | Requirement already satisfied: ordered-set<5,>4 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.1.0)
airflow-webserver-1  | Requirement already satisfied: opentelemetry-semantic-conventions==0.45b0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-sdk~=1.24.0->opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.45b0)
airflow-webserver-1  | Requirement already satisfied: importlib-resources>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from limits>=2.8->Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.4.0)
airflow-init-1       | Requirement already satisfied: idna<4,>=2.5 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.7)
airflow-init-1       | Requirement already satisfied: urllib3<3,>=1.21.1 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.26.18)
airflow-init-1       | Requirement already satisfied: certifi>=2017.4.17 in /home/airflow/.local/lib/python3.9/site-packages (from requests<3,>=2.27.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2024.2.2)
airflow-init-1       | Requirement already satisfied: greenlet!=0.4.17 in /home/airflow/.local/lib/python3.9/site-packages (from sqlalchemy<2.0,>=1.4.36->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.0.3)
airflow-init-1       | Requirement already satisfied: aiohttp>=3.9.2 in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.9.5)
airflow-init-1       | Requirement already satisfied: requests_toolbelt in /home/airflow/.local/lib/python3.9/site-packages (from apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.0.0)
airflow-init-1       | Requirement already satisfied: httpcore<0.17.0,>=0.15.0 in /home/airflow/.local/lib/python3.9/site-packages (from httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.16.3)
airflow-init-1       | Requirement already satisfied: rfc3986<2,>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from rfc3986[idna2008]<2,>=1.3->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.5.0)
airflow-init-1       | Requirement already satisfied: sniffio in /home/airflow/.local/lib/python3.9/site-packages (from httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-init-1       | Requirement already satisfied: opentelemetry-exporter-otlp-proto-grpc==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-init-1       | Requirement already satisfied: opentelemetry-exporter-otlp-proto-http==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-init-1       | Requirement already satisfied: googleapis-common-protos~=1.52 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.63.0)
airflow-init-1       | Requirement already satisfied: grpcio<2.0.0,>=1.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.63.0)
airflow-init-1       | Requirement already satisfied: opentelemetry-exporter-otlp-proto-common==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-init-1       | Requirement already satisfied: opentelemetry-proto==1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-init-1       | Requirement already satisfied: opentelemetry-sdk~=1.24.0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.24.0)
airflow-init-1       | Requirement already satisfied: protobuf<5.0,>=3.19 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-proto==1.24.0->opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.25.3)
airflow-init-1       | Requirement already satisfied: aiosignal>=1.1.2 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.3.1)
airflow-init-1       | Requirement already satisfied: frozenlist>=1.1.1 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.4.1)
airflow-init-1       | Requirement already satisfied: multidict<7.0,>=4.5 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.0.5)
airflow-init-1       | Requirement already satisfied: yarl<2.0,>=1.0 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.9.4)
airflow-init-1       | Requirement already satisfied: async-timeout<5.0,>=4.0 in /home/airflow/.local/lib/python3.9/site-packages (from aiohttp>=3.9.2->apache-airflow-providers-http->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.0.3)
airflow-init-1       | Requirement already satisfied: pycparser in /home/airflow/.local/lib/python3.9/site-packages (from cffi>=1.12->cryptography>=39.0.0->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.22)
airflow-init-1       | Requirement already satisfied: h11<0.15,>=0.13 in /home/airflow/.local/lib/python3.9/site-packages (from httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.14.0)
airflow-init-1       | Requirement already satisfied: anyio<5.0,>=3.0 in /home/airflow/.local/lib/python3.9/site-packages (from httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.3.0)
airflow-init-1       | Requirement already satisfied: exceptiongroup>=1.0.2 in /home/airflow/.local/lib/python3.9/site-packages (from anyio<5.0,>=3.0->httpcore<0.17.0,>=0.15.0->httpx->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (1.2.1)
airflow-init-1       | Requirement already satisfied: dnspython>=2.0.0 in /home/airflow/.local/lib/python3.9/site-packages (from email-validator>=1.0.5->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.6.1)
airflow-init-1       | Requirement already satisfied: Babel>=2.3 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Babel<3,>=1->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (2.14.0)
airflow-init-1       | Requirement already satisfied: limits>=2.8 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (3.11.0)
airflow-init-1       | Requirement already satisfied: ordered-set<5,>4 in /home/airflow/.local/lib/python3.9/site-packages (from Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (4.1.0)
airflow-init-1       | Requirement already satisfied: opentelemetry-semantic-conventions==0.45b0 in /home/airflow/.local/lib/python3.9/site-packages (from opentelemetry-sdk~=1.24.0->opentelemetry-exporter-otlp-proto-grpc==1.24.0->opentelemetry-exporter-otlp->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (0.45b0)
airflow-init-1       | Requirement already satisfied: importlib-resources>=1.3 in /home/airflow/.local/lib/python3.9/site-packages (from limits>=2.8->Flask-Limiter<4,>3->flask-appbuilder==4.4.1->apache-airflow-providers-fab>=1.0.2->apache-airflow>=2.6.0->apache-airflow-providers-postgres) (6.4.0)
airflow-init-1       | 
airflow-init-1       | [notice] A new release of pip is available: 24.0 -> 25.1.1
airflow-init-1       | [notice] To update, run: pip install --upgrade pip
airflow-init-1       | /home/airflow/.local/lib/python3.9/site-packages/airflow/cli/commands/db_command.py:48 DeprecationWarning: `db init` is deprecated.  Use `db migrate` instead to migrate the db and/or airflow connections create-default-connections to create the default connections
airflow-init-1       | DB: postgresql+psycopg2://airflow:***@postgres/airflow
airflow-init-1       | [2025-06-08T15:16:38.183+0000] {migration.py:216} INFO - Context impl PostgresqlImpl.
airflow-webserver-1  | 
airflow-webserver-1  | [notice] A new release of pip is available: 24.0 -> 25.1.1
airflow-webserver-1  | [notice] To update, run: pip install --upgrade pip
airflow-webserver-1  | [2025-06-08T15:16:36.973+0000] {configuration.py:2087} INFO - Creating new FAB webserver config file in: /opt/airflow/webserver_config.py
airflow-webserver-1  | /home/airflow/.local/lib/python3.9/site-packages/flask_limiter/extension.py:337 UserWarning: Using the in-memory storage for tracking rate limits as no storage was explicitly specified. This is not recommended for production use. See: https://flask-limiter.readthedocs.io#configuring-a-storage-backend for documentation about configuring the storage backend.
airflow-webserver-1  | [2025-06-08 15:16:47 +0000] [30] [INFO] Starting gunicorn 22.0.0
airflow-init-1       | [2025-06-08T15:16:38.185+0000] {migration.py:219} INFO - Will assume transactional DDL.
airflow-init-1       | [2025-06-08T15:16:38.995+0000] {migration.py:216} INFO - Context impl PostgresqlImpl.
airflow-init-1       | [2025-06-08T15:16:38.995+0000] {migration.py:219} INFO - Will assume transactional DDL.
airflow-init-1       | [2025-06-08T15:16:39.004+0000] {db.py:1623} INFO - Creating tables
airflow-init-1       | INFO  [alembic.runtime.migration] Context impl PostgresqlImpl.
airflow-init-1       | INFO  [alembic.runtime.migration] Will assume transactional DDL.
airflow-init-1       | WARNI [airflow.models.crypto] empty cryptography key - values will not be stored encrypted.
airflow-init-1       | Initialization done
airflow-init-1       | /home/airflow/.local/lib/python3.9/site-packages/flask_limiter/extension.py:337 UserWarning: Using the in-memory storage for tracking rate limits as no storage was explicitly specified. This is not recommended for production use. See: https://flask-limiter.readthedocs.io#configuring-a-storage-backend for documentation about configuring the storage backend.
airflow-init-1       | admin already exist in the db
airflow-scheduler-1  | [2025-06-08T15:16:48.706+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_test_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:16:49.375+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_csv_to_postgres_dag to None, run_after=None
airflow-webserver-1  | [2025-06-08T15:16:58.826+0000] {providers_manager.py:283} INFO - Optional provider feature disabled when importing 'airflow.providers.google.leveldb.hooks.leveldb.LevelDBHook' from 'apache-airflow-providers-google' package
airflow-init-1       | [2025-06-08T15:17:02.239+0000] {providers_manager.py:283} INFO - Optional provider feature disabled when importing 'airflow.providers.google.leveldb.hooks.leveldb.LevelDBHook' from 'apache-airflow-providers-google' package
airflow-webserver-1  | [2025-06-08 15:17:04 +0000] [30] [INFO] Listening at: http://0.0.0.0:8080 (30)
airflow-webserver-1  | [2025-06-08 15:17:04 +0000] [30] [INFO] Using worker: sync
airflow-webserver-1  | [2025-06-08 15:17:04 +0000] [46] [INFO] Booting worker with pid: 46
airflow-webserver-1  | [2025-06-08 15:17:04 +0000] [47] [INFO] Booting worker with pid: 47
airflow-webserver-1  | [2025-06-08 15:17:04 +0000] [48] [INFO] Booting worker with pid: 48
airflow-webserver-1  | [2025-06-08 15:17:04 +0000] [49] [INFO] Booting worker with pid: 49
airflow-init-1       | [2025-06-08T15:17:07.362+0000] {crypto.py:82} WARNING - empty cryptography key - values will not be stored encrypted.
airflow-init-1       | A connection with `conn_id`=write_to_psql already exists.
airflow-init-1 exited with code 0
airflow-scheduler-1  | [2025-06-08T15:17:19.675+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_test_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:17:20.800+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_csv_to_postgres_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:17:50.156+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_test_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:17:51.218+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_csv_to_postgres_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:18:20.514+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_test_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:18:22.642+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_csv_to_postgres_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:18:51.939+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_test_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:18:52.998+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_csv_to_postgres_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:19:22.334+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_test_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:19:24.429+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_csv_to_postgres_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:19:53.766+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_test_dag to None, run_after=None
airflow-scheduler-1  | [2025-06-08T15:19:54.818+0000] {dag.py:3954} INFO - Setting next_dagrun for simple_csv_to_postgres_dag to None, run_after=None


