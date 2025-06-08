from airflow import DAG
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