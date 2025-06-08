# Apache Airflow CSV to PostgreSQL Pipeline

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
git clone https://github.com/jrderek9/airflow_demo_festman.git
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
