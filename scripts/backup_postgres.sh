#!/bin/bash

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
exit 0