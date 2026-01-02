import pandas as pd
import random
import os
import datetime
from pathlib import Path

'''
NOTE: This script serves as a Synthetic Data Producer, simulating CDC (Change Data Capture) logs from an upstream OLTP production environment. 
In a production setting, this would be replaced by a tool like Debezium extracting row-level changes from the application database.
'''

# Output Directory
OUTPUT_DIR = Path("data/daily_incoming_data")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 1. Store Master Data (ID -> Region)
STORES = {
    "S001": "East",
    "S002": "West",
    "S003": "North",
    "S004": "South",
    "S005": "West"
}

# 2. Product Master Data (ID -> Category, Approx Base Price)
# Extracted from your sample data to ensure consistency
PRODUCTS = {
    "P0096": {"Category": "Toys", "BasePrice": 85.00},
    "P0016": {"Category": "Clothing", "BasePrice": 93.00},
    "P0031": {"Category": "Electronics", "BasePrice": 115.00},
    "P0159": {"Category": "Electronics", "BasePrice": 85.00},
    "P0129": {"Category": "Furniture", "BasePrice": 120.00},
    "P0116": {"Category": "Furniture", "BasePrice": 110.00},
    "P0070": {"Category": "Electronics", "BasePrice": 160.00},
    "P0171": {"Category": "Electronics", "BasePrice": 160.00},
    "P0175": {"Category": "Electronics", "BasePrice": 120.00},
    "P0046": {"Category": "Clothing", "BasePrice": 140.00},
    "P0067": {"Category": "Furniture", "BasePrice": 90.00},
    "P0183": {"Category": "Electronics", "BasePrice": 70.00},
    "P0166": {"Category": "Groceries", "BasePrice": 85.00},
    "P0079": {"Category": "Furniture", "BasePrice": 105.00},
    "P0187": {"Category": "Clothing", "BasePrice": 145.00},
    "P0178": {"Category": "Clothing", "BasePrice": 120.00},
    "P0057": {"Category": "Clothing", "BasePrice": 125.00},
    "P0153": {"Category": "Furniture", "BasePrice": 75.00},
    "P0083": {"Category": "Toys", "BasePrice": 110.00},
    "P0069": {"Category": "Clothing", "BasePrice": 200.00},
    "P0125": {"Category": "Clothing", "BasePrice": 125.00},
    "P0017": {"Category": "Toys", "BasePrice": 110.00},
    "P0149": {"Category": "Furniture", "BasePrice": 135.00},
    "P0094": {"Category": "Groceries", "BasePrice": 160.00},
    "P0066": {"Category": "Clothing", "BasePrice": 135.00},
    "P0061": {"Category": "Clothing", "BasePrice": 170.00},
    "P0085": {"Category": "Electronics", "BasePrice": 130.00},
    "P0068": {"Category": "Electronics", "BasePrice": 125.00},
    "P0126": {"Category": "Clothing", "BasePrice": 185.00},
    "P0133": {"Category": "Clothing", "BasePrice": 190.00}
}

# 3. Context Lists
WEATHER_OPTS = ["Sunny", "Rainy", "Cloudy", "Snowy"]
DISCOUNT_OPTS = [0, 5, 10, 15, 20]

def get_season(date_obj):
    """Determine season based on month."""
    m = date_obj.month
    if m in [12, 1, 2]: return "Winter"
    elif m in [3, 4, 5]: return "Spring"
    elif m in [6, 7, 8]: return "Summer"
    else: return "Autumn"

def generate_daily_batch(target_date=None):
    # Generates a csv file for a specific date containing 150 rows (5 Stores * 30 Products)
    
    date_str_csv = target_date.strftime("%d/%m/%y") # DD/MM/YY format inside CSV
    date_str_file = target_date.strftime("%Y%m%d")  # YYYYMMDD format for filename
    
    rows = []
    
    print(f"--- Generating Data for {date_str_csv} ---")

    # Cartesian Product (Every Store sells Every Product)
    for store_id, region in STORES.items():
        for prod_id, prod_info in PRODUCTS.items():
            # --- Simulation Logic ---

            # Inventory & Sales
            inventory = random.randint(20, 300)
            
            # Units sold can't be more than inventory
            units_sold = random.randint(0, min(inventory, 150))
            
            # Replenishment logic
            units_ordered = random.randint(0, 150)
            
            # Pricing (Base price + random variance)
            base_price = prod_info["BasePrice"]
            price_variance = random.uniform(0.85, 1.15)
            final_price = round(base_price * price_variance, 2)
            
            # Competitor Price (Usually similar)
            comp_price = round(final_price * random.uniform(0.95, 1.05), 2)
            
            # Demand Forecast (Simulate an ML model prediction)
            # Forecast is usually Sales +/- noise
            forecast_noise = random.uniform(0.8, 1.2)
            demand_forecast = round(units_sold * forecast_noise, 2)
            
            # Promotion (Randomly 1 or 0)
            is_promo = random.choice([0, 0, 0, 1]) # 25% chance
            
            # Create Row Dictionary matching Schema
            row = {
                "Date": date_str_csv,
                "Store ID": store_id,
                "Product ID": prod_id,
                "Category": prod_info["Category"],
                "Region": region,
                "Inventory Level": inventory,
                "Units Sold": units_sold,
                "Units Ordered": units_ordered,
                "Demand Forecast": demand_forecast,
                "Price": final_price,
                "Discount": random.choice(DISCOUNT_OPTS),
                "Weather Condition": random.choice(WEATHER_OPTS),
                "Holiday/Promotion": is_promo,
                "Competitor Pricing": comp_price,
                "Seasonality": get_season(target_date)
            }
            rows.append(row)

    # 3. Create DataFrame and Save
    df = pd.DataFrame(rows)
    
    # Save as data/incoming/20220104.csv (Professional naming)
    filename = OUTPUT_DIR / f"{date_str_file}.csv"
    
    df.to_csv(filename, index=False)
    print(f"Successfully generated {len(df)} rows.")
    print(f"File saved to: {filename}")

if __name__ == "__main__":
    # Example: Generate data for a specific simulation date
    # You can change this date to test the pipeline
    simulation_date = datetime.date(2024, 1, 1) 
    generate_daily_batch(simulation_date)