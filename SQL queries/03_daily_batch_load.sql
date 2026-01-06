/* ================================================================================
   BATCH UPDATE SCRIPT
   Logic: Run daily @ 1:00 AM to process yesterday's data
   ================================================================================ */

BEGIN;
    -- STEP 1: CREATE TEMPORARY STAGING AREA
    -- filtering for last date to apppend in the tables with new data
    CREATE TEMP TABLE tmp_daily_batch AS
    SELECT * 
	FROM raw.inventory_dump
    WHERE TO_DATE("Date", 'DD/MM/YY') = (
    	SELECT MAX(TO_DATE("Date", 'DD/MM/YY'))
    	FROM raw.inventory_dump
	); 

    -- STEP 2: DIMENSION SYNCHRONIZATION

    -- (a) Sync Stores
    INSERT INTO dwh.dim_store (store_id, region, store_status)
    SELECT DISTINCT 
        "Store ID", 
        "Region", 
        'Active'
    FROM tmp_daily_batch
    WHERE "Store ID" IS NOT NULL
    ON CONFLICT (store_id) 
    DO UPDATE SET 
        store_status = 'Active'
    WHERE dwh.dim_store.store_status = 'Inactive';

    -- (b) Sync Products
    INSERT INTO dwh.dim_product (product_id, category, product_status)
    SELECT DISTINCT 
        "Product ID", 
        "Category", 
        'Active'
    FROM tmp_daily_batch
    WHERE "Product ID" IS NOT NULL
    ON CONFLICT (product_id) 
    DO UPDATE SET
        product_status = 'Active'
    WHERE dwh.dim_product.product_status = 'Inactive';

    -- (c) Sync Dates
    INSERT INTO dwh.dim_date(date_key, full_date, month, year, season, is_holiday, weekday_name)
    SELECT 
        TO_CHAR(datum, 'YYYYMMDD')::int,
        datum,
        EXTRACT(month from datum),
        EXTRACT(year from datum),
        season,
        FALSE, -- Default false
        TO_CHAR(datum, 'Day')
    FROM (
        SELECT DISTINCT TO_DATE("Date", 'DD/MM/YY') as datum, "Seasonality" as season
        FROM tmp_daily_batch
    ) dates
    ON CONFLICT (date_key) DO NOTHING;

    -- STEP 3: FACT TABLE LOAD
    INSERT INTO dwh.fct_inventory_daily(
        date_key, store_key, product_key, inventory_level, units_sold, units_ordered, 
        demand_forecast, selling_price, discount_applied, competitor_price, weather_condition, is_promotion_active
    )
    SELECT 
        TO_CHAR(TO_DATE(r."Date", 'DD/MM/YY'), 'YYYYMMDD')::INT,
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
        CASE WHEN r."Holiday/Promotion" = '1' THEN TRUE ELSE FALSE END
    FROM tmp_daily_batch r
    JOIN dwh.dim_store s ON r."Store ID" = s.store_id
    JOIN dwh.dim_product p ON r."Product ID" = p.product_id
    ON CONFLICT (date_key, store_key, product_key) 
    DO NOTHING; 

    -- Step 4: UPDATING DEMAND FORECAST (7-DAY FUTURE SNAPSHOT)
    Truncate dwh.demand_forecast_7d;

    INSERT INTO dwh.demand_forecast_7d (forecasted_on, full_date, store_id, product_id, category, region, selling_price, discount_applied, weather_condition, is_promotion_active, competitor_price, season, demand_forecast)
    SELECT
        TO_DATE("Forecasted On", 'DD/MM/YY') as forecasted_on,
        TO_DATE("Date", 'DD/MM/YY') as full_date,
        "Store ID" as store_id,
        "Product ID" as product_id,
        "Category" as category,
        "Region" as region,
        NULLIF("Price", '')::DECIMAL(10,2) as selling_price,
        NULLIF("Discount", '')::DECIMAL(10,2) as discount_applied,
        "Weather Condition" as weather_condition,
        CASE WHEN "Holiday/Promotion" = '1' THEN TRUE 
            ELSE FALSE 
        END as is_promotion_active,
        NULLIF("Competitor Pricing", '')::DECIMAL(10,2) as competitor_price,
        "Seasonality" as season,
        NULLIF("Demand Forecast", '')::DECIMAL(10,2) as demand_forecast
    FROM raw.demand_forecast_7d;
    
    -- Clean up
    DROP TABLE tmp_daily_batch;

COMMIT;