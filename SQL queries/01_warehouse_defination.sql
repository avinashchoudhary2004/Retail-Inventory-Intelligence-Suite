/* ================================================================================
WAREHOUSE DEFINITION SCRIPT
Purpose: Initialize Database Schemas and Table Structures (DDL)
================================================================================ */

-- 1. Create the Schemas (The Layers)
CREATE SCHEMA IF NOT EXISTS raw; -- Loading the raw data from python script
CREATE SCHEMA IF NOT EXISTS dwh; -- Warehouse Core (dimension + facts)
CREATE SCHEMA IF NOT EXISTS rpt; -- Reporting/Analytics (snapshots for dashboards and decision making)

-- 2. Create the RAW Table (Matches CSV columns exactly)
CREATE TABLE raw.inventory_dump (
    "Date" TEXT,
    "Store ID" TEXT,
    "Product ID" TEXT,
    "Category" TEXT,
    "Region" TEXT,
    "Inventory Level" TEXT,
    "Units Sold" TEXT,
    "Units Ordered" TEXT,
    "Demand Forecast" TEXT,
    "Price" TEXT,
    "Discount" TEXT,
    "Weather Condition" TEXT,
    "Holiday/Promotion" TEXT,
    "Competitor Pricing" TEXT,
    "Seasonality" TEXT
);

-- 3. Create the dwh tables
-- (a) Dimension: Date (Static, no SCD needed)
CREATE TABLE dwh.dim_date (
    date_key INT PRIMARY KEY, -- YYYYMMDD
    full_date DATE, 
    month INT,
    year INT,
    season VARCHAR(20),
    is_holiday BOOLEAN,
	weekday_name VARCHAR(10)
);

-- (b) Dimension: Store (Static, no SCD needed)
CREATE TABLE dwh.dim_store (
    store_key    SERIAL PRIMARY KEY,        -- surrogate key
    store_id     VARCHAR(10) UNIQUE NOT NULL, 
    region       VARCHAR(50) NOT NULL,
    store_status VARCHAR(10) NOT NULL
);


-- (c) Dimension: Product (SCD Type 1 - History Not Required)
CREATE TABLE dwh.dim_product (
    product_key SERIAL PRIMARY KEY,  
    product_id VARCHAR(10) UNIQUE NOT NULL,     
    category VARCHAR(50) NOT NULL,
	product_status VARCHAR(10) NOT NULL
);

-- (d) Fact Table: Inventory & Sales
CREATE TABLE dwh.fct_inventory_daily (
    -- Foreign Keys
    date_key INT,
    store_key INT,
    product_key INT,
    -- Metrics
    inventory_level INT,
    units_sold INT,
    units_ordered INT,
    demand_forecast DECIMAL(10,2),
    selling_price DECIMAL(10,2),
    discount_applied DECIMAL(10,2),
    competitor_price DECIMAL(10,2),
    -- Degenerate Dimensions (Context that lives in Fact)
    weather_condition VARCHAR(50),
	is_promotion_active boolean,
	-- Constraints
	Primary key (date_key, store_key, product_key),
    CONSTRAINT fk_inv_daily_date 
        FOREIGN KEY (date_key) 
        REFERENCES dwh.dim_date(date_key),
    CONSTRAINT fk_inv_daily_store 
        FOREIGN KEY (store_key) 
        REFERENCES dwh.dim_store(store_key),
    CONSTRAINT fk_inv_daily_product 
        FOREIGN KEY (product_key) 
        REFERENCES dwh.dim_product(product_key)
);
