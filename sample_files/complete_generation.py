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
    'transaction_id': [f"TRX{str(i + 1).zfill(8)}" for i in range(num_rows)],
    'order_date': [(datetime.now() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_rows)],
    'customer_id': [f"CUST{str(random.randint(1, 50000)).zfill(6)}" for _ in range(num_rows)],
    'customer_name': [f"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}" for _ in range(num_rows)],
    'customer_email': [f"customer{random.randint(1, 50000)}@email.com" for _ in range(num_rows)],
    'customer_type': np.random.choice(['Regular', 'Premium', 'VIP'], num_rows),
    'product_ids': [';'.join([f"PROD{random.randint(1, 100000)}" for _ in range(random.randint(1, 3))]) for _ in range(num_rows)],
    'product_names': [';'.join([f"Product {random.randint(1, 1000)}" for _ in range(random.randint(1, 3))]) for _ in range(num_rows)],
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
    'ticket_id': [f"TICKET{str(i + 1).zfill(7)}" for i in range(num_rows)],
    'created_date': [(datetime.now() - timedelta(days=random.randint(0, 180))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_rows)],
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
    'session_id': [f"SESSION{str(i + 1).zfill(10)}" for i in range(num_rows)],
    'user_id': [f"USER{str(random.randint(1, 100000)).zfill(8)}" if random.random() < 0.7 else 'anonymous' for _ in range(num_rows)],
    'timestamp': [(datetime.now() - timedelta(hours=random.randint(0, 720))).strftime('%Y-%m-%d %H:%M:%S') for _ in range(num_rows)],
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
    'ip_address_hash': [f"hash_{random.randint(100000, 999999)}" for _ in range(num_rows)],
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
    'patient_id': [f"PAT{str(i + 1).zfill(8)}" for i in range(num_rows)],
    'admission_date': [(datetime.now().date() - timedelta(days=random.randint(0, 365))).strftime('%Y-%m-%d') for _ in range(num_rows)],
    'discharge_date': [(datetime.now().date() - timedelta(days=random.randint(0, 7))).strftime('%Y-%m-%d') for _ in range(num_rows)],
    'patient_name': [f"{random.choice(['John', 'Jane', 'Mike'])} {random.choice(['Smith', 'Doe', 'Johnson'])}" for _ in range(num_rows)],
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

df = pd.DataFrame(data)
df.to_csv('healthcare_records.csv', index=False)
print(f'✅ Created healthcare_records.csv with {len(df)} rows')

print('\nAll CSV files generated successfully!')
print('Total files: 6')
print('Files location: /opt/airflow/sample_files/')
