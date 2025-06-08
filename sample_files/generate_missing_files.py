import pandas as pd
import numpy as np
from datetime import datetime, timedelta
import random
import os

# Set random seed
np.random.seed(42)
random.seed(42)

# Check which files exist
existing_files = [f for f in os.listdir('.') if f.endswith('.csv')]
print(f'Existing files: {existing_files}')

# Generate sales_transactions.csv if missing
if 'sales_transactions.csv' not in existing_files:
    print('Generating sales_transactions.csv...')
    num_rows = 150000
    
    payment_methods = ['Credit Card', 'Debit Card', 'PayPal', 'Cash', 'Bank Transfer']
    channels = ['Online', 'In-Store', 'Mobile App', 'Phone']
    statuses = ['Delivered', 'Shipped', 'Processing', 'Cancelled', 'Refunded']
    
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
    # Fix refund amount for refunded orders
    df.loc[df['order_status'] == 'Refunded', 'refund_amount'] = df.loc[df['order_status'] == 'Refunded', 'total_amount']
    df.to_csv('sales_transactions.csv', index=False)
    print(f'✅ Created sales_transactions.csv with {len(df)} rows')

# Generate customer_support_tickets.csv if missing
if 'customer_support_tickets.csv' not in existing_files:
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

print('\nAll files generated successfully!')
print('Files in directory:')
for f in sorted(os.listdir('.')):
    if f.endswith('.csv'):
        size = os.path.getsize(f) / (1024*1024)  # MB
        print(f'  {f}: {size:.1f} MB')
