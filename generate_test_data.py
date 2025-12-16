import pandas as pd
import numpy as np
import random
from datetime import datetime, timedelta

# Settings
NUM_ROWS = 150
np.random.seed(42)  # For reproducible chaos

# 1. Generate clean base data
products = {
    'Electronics': ['Laptop', 'Headphones', 'Smartphone', 'Monitor'],
    'Furniture': ['Chair', 'Desk', 'Bookshelf', 'Lamp'],
    'Office': ['Paper', 'Binder', 'Pen', 'Stapler']
}

data = []
start_date = datetime(2024, 1, 1)

for i in range(NUM_ROWS):
    category = random.choice(list(products.keys()))
    product = random.choice(products[category])
    
    # Normalish values
    sales = round(np.random.uniform(10, 500), 2)
    quantity = np.random.randint(1, 10)
    discount = round(np.random.uniform(0, 0.3), 2)
    profit = round(sales * 0.1, 2)
    
    row = {
        'Order_ID': f"ORD-{1000+i}",
        'Date': (start_date + timedelta(days=np.random.randint(0, 365))).strftime('%Y-%m-%d'),
        'Category': category,
        'Product': product,
        'Sales': sales,
        'Quantity': quantity,
        'Discount': discount,
        'Profit': profit,
        'Region': random.choice(['North', 'South', 'East', 'West']),
        'Customer_Age': np.random.randint(18, 70)
    }
    data.append(row)

df = pd.DataFrame(data)

# 2. INJECT POISON (The Errors for the Video) ☠️

# A. Null Values (The Skeptic hates these)
# Set 15 random products to NaN
df.loc[df.sample(15).index, 'Product'] = np.nan
# Set 10 random regions to NaN
df.loc[df.sample(10).index, 'Region'] = np.nan

# B. Negative Values (Impossible data)
# Set 5 random sales to negative numbers (e.g. -500)
df.loc[df.sample(5).index, 'Sales'] = df['Sales'] * -1
# Set 5 random ages to negative (e.g. -25)
df.loc[df.sample(5).index, 'Customer_Age'] = df['Customer_Age'] * -1

# C. Massive Outliers (for the graph to look weird)
# Make one Laptop cost $50,000
df.loc[0, 'Sales'] = 50000 
df.loc[0, 'Product'] = 'Golden Laptop'

# D. Duplicates
# Duplicate the first 5 rows and append them to the end
duplicates = df.head(5).copy()
df = pd.concat([df, duplicates], ignore_index=True)

# 3. Save
filename = "dirty_sales_data.csv"
df.to_csv(filename, index=False)
print(f"✅ Generated {filename} with {len(df)} rows.")
print(f"   - Contains NULLS, DUPLICATES, NEGATIVES, and OUTLIERS.")