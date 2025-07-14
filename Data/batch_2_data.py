# use this for second batch because it has correct city and state mapping

import os
import math
import pandas as pd
import numpy as np
import glob

# ------------------ Data Generation Function ------------------

def generate_data(num_rows, table_name, customers=None, orders=None, items=None):
    """Generates a DataFrame based on table_name and returns it."""

    if table_name == "customers":
        city_state_map = {
            'Mumbai': 'Maharashtra', 'Delhi': 'Delhi', 'Bangalore': 'Karnataka',
            'Chennai': 'Tamil Nadu', 'Kolkata': 'West Bengal', 'Hyderabad': 'Telangana',
            'Pune': 'Maharashtra', 'Ahmedabad': 'Gujarat', 'Patna': 'Bihar',
            'Jaipur': 'Rajasthan', 'Lucknow': 'Uttar Pradesh', 'Noida': 'Uttar Pradesh',
            'Nagpur': 'Maharashtra', 'Indore': 'Madhya Pradesh', 'chandigarh': 'Punjab'
        }
        cities = np.random.choice(list(city_state_map.keys()), num_rows)
        states = [city_state_map[city] for city in cities]

        data = pd.DataFrame({
            'customer_id': np.arange(num_rows),
            'name': [f'Customer_{i}' for i in range(num_rows)],
            'city': cities,
            'state': states,
            'country': 'India',
            'registration_date': pd.to_datetime('2023-01-01') + pd.to_timedelta(np.random.randint(0, 365, num_rows), unit='D'),
            'is_active': np.random.choice([True, False], num_rows)
        })

    elif table_name == "orders":
        if customers is None:
            raise ValueError("Customer data must be provided for generating orders.")
        data = pd.DataFrame({
            'order_id': np.arange(num_rows),
            'customer_id': np.random.choice(customers['customer_id'], num_rows),
            'order_date': pd.to_datetime('2024-01-01') + pd.to_timedelta(np.random.randint(0, 365, num_rows), unit='D'),
            'total_amount': np.random.uniform(10, 1000, num_rows),
            'status': np.random.choice(['Pending', 'Shipped', 'Delivered', 'Cancelled'], num_rows)
        })

    elif table_name == "items":
        data = pd.DataFrame({
            'item_id': np.arange(num_rows),
            'item_name': [f'Item_{i}' for i in range(num_rows)],
            'category': np.random.choice(['Electronics', 'Clothing', 'Books', 'Home', 'Sports'], num_rows),
            'price': np.round(np.random.uniform(5, 500, num_rows), 2)
        })

    elif table_name == "order_lines":
        if orders is None or items is None:
            raise ValueError("Orders and items must be provided for generating order lines.")
        data = pd.DataFrame({
            'order_id': np.random.choice(orders['order_id'], num_rows),
            'item_id': np.random.choice(items['item_id'], num_rows),
            'quantity': np.random.randint(1, 5, num_rows),
            'unit_price': np.round(np.random.uniform(5, 500, num_rows), 2)
        })

    elif table_name == "payments":
        if orders is None:
            raise ValueError("Orders must be provided for generating payments.")
        data = pd.DataFrame({
            'payment_id': np.arange(num_rows),
            'order_id': np.random.choice(orders['order_id'], num_rows),
            'payment_date': pd.to_datetime('2024-01-01') + pd.to_timedelta(np.random.randint(0, 365, num_rows), unit='D'),
            'amount': np.round(np.random.uniform(10, 1000, num_rows), 2),
            'payment_method': np.random.choice(['Credit Card', 'Debit Card', 'PayPal', 'UPI'], num_rows),
            'status': np.random.choice(['Success', 'Failed'], num_rows)
        })

    elif table_name == "shippings":
        if orders is None:
            raise ValueError("Orders must be provided for generating shippings.")
        data = pd.DataFrame({
            'shipping_id': np.arange(num_rows),
            'order_id': np.random.choice(orders['order_id'], num_rows),
            'shipping_date': pd.to_datetime('2024-01-01') + pd.to_timedelta(np.random.randint(0, 365, num_rows), unit='D'),
            'shipping_address': [f'Address_{i}' for i in range(num_rows)],
            'shipping_method': np.random.choice(['Standard', 'Express'], num_rows),
            'shipping_status': np.random.choice(['In Transit', 'Delivered'], num_rows)
        })

    else:
        return None

    return data

# ------------------ Utility Functions ------------------

def write_csv(data, file_path):
    """Writes DataFrame to a CSV file."""
    data.to_csv(file_path, index=False)

def estimate_row_size(data):
    """Estimate the average row size in bytes."""
    buffer = data.head(10)
    tmp_file = "temp_estimation.csv"
    buffer.to_csv(tmp_file, index=False)
    file_size = os.path.getsize(tmp_file)
    os.remove(tmp_file)
    avg_row_size = file_size / len(buffer)
    return avg_row_size

# ------------------ Main Dataset Generator ------------------

def generate_ecommerce_data(file_sizes_mb, output_dir):
    """Generates all CSVs needed for Power BI dashboard."""
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    for size_mb in file_sizes_mb:
        size_dir = os.path.join(output_dir, f"{size_mb}MB")
        os.makedirs(size_dir, exist_ok=True)

        print(f"\n📦 Generating {size_mb}MB dataset...")

        customers_temp = generate_data(10000, "customers")
        avg_row_size = estimate_row_size(customers_temp)
        target_size_bytes = size_mb * 1024 * 1024
        num_rows = math.ceil(target_size_bytes / avg_row_size)

        # Step 1: Generate data with correct dependencies
        customers = generate_data(num_rows, "customers")
        orders = generate_data(num_rows, "orders", customers=customers)
        items = generate_data(num_rows, "items")
        order_lines = generate_data(num_rows * 2, "order_lines", orders=orders, items=items)  # allow multiple lines per order
        payments = generate_data(num_rows, "payments", orders=orders)
        shippings = generate_data(num_rows, "shippings", orders=orders)

        # Step 2: Write to CSV
        write_csv(customers, os.path.join(size_dir, "customers.csv"))
        write_csv(orders, os.path.join(size_dir, "orders.csv"))
        write_csv(items, os.path.join(size_dir, "items.csv"))
        write_csv(order_lines, os.path.join(size_dir, "order_lines.csv"))
        write_csv(payments, os.path.join(size_dir, "payments.csv"))
        write_csv(shippings, os.path.join(size_dir, "shippings.csv"))

        # Step 3: Report sizes
        for name in ["customers", "orders", "items", "order_lines", "payments", "shippings"]:
            path = os.path.join(size_dir, f"{name}.csv")
            size = os.path.getsize(path) / (1024 * 1024)
            print(f"✅ {name}.csv: {size:.2f} MB")

# ------------------ Execution ------------------

file_sizes_mb = [1, 10, 150, 300]
output_directory = r"D:\data\datagen_env\dataset"

generate_ecommerce_data(file_sizes_mb, output_directory)

# Preview one sample file
sample_file = glob.glob(os.path.join(output_directory, "**/*.csv"), recursive=True)[0]
print(f"\n📄 Sample data from: {sample_file}")
print(pd.read_csv(sample_file).head())
