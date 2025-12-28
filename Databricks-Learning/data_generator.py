import boto3
import pandas as pd
import random
from faker import Faker
from io import StringIO
from datetime import datetime

# ---------- CONFIG ----------
BUCKET_NAME = "learning-databricks-0001"
TABLES = ["customers", "orders", "sales"]
ROWS_CUSTOMERS = 1000
ROWS_ORDERS = 5000
ROWS_SALES = 5000

fake = Faker()
s3 = boto3.client("s3")

# ---------- DATA GENERATORS ----------
def generate_customers(n):
    data = []
    for _ in range(n):
        data.append({
            "customer_id": fake.random_int(min=1000, max=9999),
            "name": fake.name(),
            "email": fake.email(),
            "country": fake.country(),
            "signup_date": fake.date_this_year()
        })
    return pd.DataFrame(data)

def generate_orders(customers_df, n):
    customer_ids = customers_df['customer_id'].tolist()
    products = ["Laptop", "Phone", "Tablet", "Monitor", "Headphones"]
    data = []
    for _ in range(n):
        customer_id = random.choice(customer_ids)
        quantity = random.randint(1,5)
        price = round(random.uniform(100,2000),2)
        data.append({
            "order_id": fake.uuid4(),
            "customer_id": customer_id,
            "order_date": fake.date_this_year(),
            "product": random.choice(products),
            "quantity": quantity,
            "price": price,
            "order_status": random.choice(["PLACED", "SHIPPED", "DELIVERED", "CANCELLED"])
        })
    return pd.DataFrame(data)

def generate_sales(orders_df, n):
    data = []
    for i in range(n):
        order = orders_df.iloc[i % len(orders_df)]
        total_amount = order['quantity'] * order['price']
        discount = round(total_amount * random.uniform(0,0.2),2)
        tax = round((total_amount - discount) * 0.1,2)
        final_amount = total_amount - discount + tax
        data.append({
            "sale_id": fake.uuid4(),
            "order_id": order['order_id'],
            "sale_date": order['order_date'],
            "total_amount": total_amount,
            "discount": discount,
            "tax": tax,
            "final_amount": final_amount
        })
    return pd.DataFrame(data)

# ---------- UPLOAD FUNCTION ----------
def upload_to_s3(df, table_name, load_date):
    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False)
    s3_key = f"raw/{table_name}/{load_date}/{table_name}.csv"
    s3.put_object(
        Bucket=BUCKET_NAME,
        Key=s3_key,
        Body=csv_buffer.getvalue()
    )
    print(f"✅ Uploaded {table_name} → s3://{BUCKET_NAME}/{s3_key}")

# ---------- MAIN ----------
if __name__ == "__main__":
    today = datetime.today().strftime("%Y-%m-%d")
    
    # Generate and upload customers
    customers_df = generate_customers(ROWS_CUSTOMERS)
    upload_to_s3(customers_df, "customers", today)
    
    # Generate and upload orders
    orders_df = generate_orders(customers_df, ROWS_ORDERS)
    upload_to_s3(orders_df, "orders", today)
    
    # Generate and upload sales
    sales_df = generate_sales(orders_df, ROWS_SALES)
    upload_to_s3(sales_df, "sales", today)
