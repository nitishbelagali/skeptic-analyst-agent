import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

def generate_data(num_rows=200):
    # 1. Setup Categories (Dimensions)
    products = ['Laptop', 'Mouse', 'Monitor', 'Keyboard', 'Headphones', 'Phone', 'Charger', 'Webcam']
    categories = ['Electronics', 'Accessories']
    regions = ['North', 'South', 'East', 'West']
    payment_methods = ['Credit Card', 'PayPal', 'Cash', 'Debit Card']
    
    data = []
    
    # 2. Generate Rows
    for i in range(num_rows):
        date = datetime(2024, 1, 1) + timedelta(days=random.randint(0, 365))
        prod = random.choice(products)
        cat = 'Electronics' if prod in ['Laptop', 'Phone', 'Monitor'] else 'Accessories'
        reg = random.choice(regions)
        pay = random.choice(payment_methods)
        
        # Facts (Numeric)
        qty = random.randint(1, 10)
        price = random.uniform(10.0, 1000.0)
        discount = random.choice([0, 0.05, 0.1, 0.2])
        sales = (price * qty) * (1 - discount)
        rating = random.randint(1, 5)
        
        data.append([i+1, date.date(), prod, cat, reg, pay, qty, round(sales, 2), discount, rating])

    df = pd.DataFrame(data, columns=[
        'transaction_id', 'date', 'product', 'category', 'region', 
        'payment_method', 'quantity', 'sales_amount', 'discount_pct', 'customer_rating'
    ])
    
    # 3. Inject Errors (For the Auditor to catch)
    # Inject Nulls in 'payment_method'
    for _ in range(5):
        df.loc[random.randint(0, num_rows-1), 'payment_method'] = "" 
        
    # Inject Negatives in 'sales_amount'
    for _ in range(3):
        df.loc[random.randint(0, num_rows-1), 'sales_amount'] = -100.00
        
    # Inject Outlier in 'quantity'
    df.loc[0, 'quantity'] = 1000
    
    # Inject Duplicates
    df = pd.concat([df, df.iloc[:3]], ignore_index=True)
    
    # Save
    df.to_csv("large_sales_test.csv", index=False)
    print(f"✅ Generated 'large_sales_test.csv' with {len(df)} rows and {len(df.columns)} columns.")

if __name__ == "__main__":
    generate_data()