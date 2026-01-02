/* ================================================================================
HISTORIC DATA LOAD SCRIPT
Purpose: Initial migration from raw.inventory_dump to Star Schema (DWH)
================================================================================ */

-- 1. Insert historic data in dim_product
INSERT INTO dwh.dim_product(product_id, category, product_status)
SELECT DISTINCT 
    "Product ID", 
    "Category",
    'active' AS product_status
FROM raw.inventory_dump;

-- 2. Insert historic data into dim_store
INSERT INTO dwh.dim_store(store_id, region, store_status)
SELECT DISTINCT 
    "Store ID", 
    "Region",
    'Active' AS product_status
FROM raw.inventory_dump;

-- 3. Insert historic data into dim_date
INSERT INTO dwh.dim_date(date_key, full_date, month, year, season, is_holiday, weekday_name)
Select to_char(date, 'YYYYMMDD')::int,
	date,
	extract(month from date),
	extract(year from date),
	season,
	False,
	to_char(date, 'Day')
from (
	select distinct to_date("Date", 'dd/mm/yy') as date,
		"Seasonality" as season
	from raw.inventory_dump
	order by date
);

-- 4. Insert historic date into fct_inventory_daily
INSERT INTO dwh.fct_inventory_daily(date_key, store_key, product_key, inventory_level, units_sold, units_ordered, 
	demand_forecast, selling_price, discount_applied, competitor_price, weather_condition, is_promotion_active)
SELECT 
    TO_CHAR(TO_DATE(r."Date", 'DD/MM/YY'), 'YYYYMMDD')::INT as date_key,
    s.store_key,
    p.product_key,
    r."Inventory Level"::INT,
    r."Units Sold"::INT,
    r."Units Ordered"::INT,
    r."Demand Forecast"::DECIMAL,
    r."Price"::DECIMAL,
    r."Discount"::DECIMAL,
    r."Competitor Pricing"::DECIMAL,
    r."Weather Condition",
    CASE WHEN r."Holiday/Promotion" = '1' THEN TRUE 
		ELSE FALSE 
	END
FROM raw.inventory_dump r
JOIN dwh.dim_store s ON r."Store ID" = s.store_id
JOIN dwh.dim_product p ON r."Product ID" = p.product_id
ON CONFLICT (date_key, product_key, store_key) DO NOTHING
;


