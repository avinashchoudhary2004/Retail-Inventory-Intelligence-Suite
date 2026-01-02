/* ================================================================================
  PURPOSE: Defines the Reporting Layer (RPT schema) for the Retail Inventory Dashboard.
  
  CONTENTS:
  1. view_inventory_daily_snapshot   - One Big Table (OBT) for generic analysis
  2. view_inventory_health_final     - Current stock status & alerts
  3. view_forecast_accuracy_daily    - ML Model performance monitoring
  4. view_inventory_turnover_monthly - Capital efficiency & turnover rates
================================================================================
*/

-- 1. Inventory daily snapshot
CREATE OR REPLACE VIEW rpt.inventory_daily_snapshot AS
select d.full_date,
	s.store_id,
	s.region,
	p.product_id,
	p.category,
	f.units_sold,
	f.units_ordered,
	f.inventory_level,
	f.selling_price,
	f.discount_applied,
	f.weather_condition,
	f.is_promotion_active
from dwh.fct_inventory_daily f
join dwh.dim_date d
	on f.date_key = d.date_key
join dwh.dim_product p
	on f.product_key = p.product_key
join dwh.dim_store s
	on f.store_key = s.store_key
;

-- 2. Inventory Turnover
CREATE OR REPLACE VIEW rpt.inventory_turnover_monthly AS
WITH monthly_stats AS (
    SELECT 
        d.year,
        d.month,
        s.store_id,
        s.region,
        p.product_id,
        p.category,        
        SUM(f.units_sold) AS total_units_sold,
        SUM(f.inventory_level) AS total_inventory_sum,
        COUNT(DISTINCT d.date_key) AS days_observed
    FROM dwh.fct_inventory_daily f
    JOIN dwh.dim_date d ON f.date_key = d.date_key
    JOIN dwh.dim_store s ON f.store_key = s.store_key
    JOIN dwh.dim_product p ON f.product_key = p.product_key
    GROUP BY 1, 2, 3, 4, 5, 6
)
SELECT 
    year,
    month,
	store_id,
    product_id,
	category,
    total_units_sold,
    ROUND(total_inventory_sum / NULLIF(days_observed, 0), 0) AS avg_inventory_held,
    ROUND(
        	total_units_sold / NULLIF((total_inventory_sum / NULLIF(days_observed, 0)), 0), 
    		2
	) AS turnover_ratio
FROM monthly_stats
;

select * from dwh.dim_date;

-- 3. Inventory status
-- Logic: 
--   1. Historical DoS = Inventory / (7-Day Avg Sales)
--   2. Forecast Ratio = Inventory / (Next Day Forecast)
CREATE OR REPLACE VIEW rpt.inventory_health AS
WITH max_fact_date AS (
    SELECT MAX(d.full_date) AS max_date
    FROM dwh.fct_inventory_daily f
    JOIN dwh.dim_date d ON f.date_key = d.date_key
),
velocity_7d AS (
    SELECT 
        f.store_key,
        f.product_key,
        SUM(f.units_sold)::numeric / 7 AS avg_daily_sales_7d
    FROM dwh.fct_inventory_daily f
    JOIN dwh.dim_date d ON f.date_key = d.date_key
    CROSS JOIN max_fact_date m
    WHERE d.full_date > m.max_date - INTERVAL '7 days'
    GROUP BY f.store_key, f.product_key
)
SELECT 
    d.full_date,
    s.store_id,
    s.region,
	p.product_id,
    p.category,
    f.inventory_level + f.units_ordered as effective_inventory_level,
    ROUND(v.avg_daily_sales_7d, 1) AS hist_velocity_daily,
    CASE 
        WHEN v.avg_daily_sales_7d = 0 THEN 999 
        ELSE ROUND((f.inventory_level + f.units_ordered)/ v.avg_daily_sales_7d, 1) 
    END AS historical_dos,
    f.demand_forecast AS next_day_demand,-- this need to be replaced for next day demand got from ml model
    CASE 
        WHEN f.demand_forecast = 0 THEN 999
        ELSE ROUND(f.inventory_level / f.demand_forecast, 2)
    END AS forecast_coverage_ratio,
    CASE 
        WHEN (f.inventory_level + f.units_ordered) = 0 and v.avg_daily_sales_7d <> 0 THEN 'STOCKOUT'
		WHEN (f.inventory_level + f.units_ordered) > 0 and v.avg_daily_sales_7d = 0 THEN 'DEAD STOCK'
		WHEN (f.inventory_level + f.units_ordered) = 0 and v.avg_daily_sales_7d = 0 THEN 'INACTIVE'
        WHEN ((f.inventory_level + f.units_ordered) / NULLIF(v.avg_daily_sales_7d, 0)) < 1 THEN 'CRITICAL'
        WHEN ((f.inventory_level + f.units_ordered) / NULLIF(v.avg_daily_sales_7d, 0)) < 2 THEN 'LOW STOCK'
        WHEN ((f.inventory_level + f.units_ordered) / NULLIF(v.avg_daily_sales_7d, 0)) < 5 THEN 'HEALTHY'
        WHEN ((f.inventory_level + f.units_ordered) / NULLIF(v.avg_daily_sales_7d, 0)) >= 5 THEN 'OVERSTOCK'
        ELSE 'UNKNOWN'
    END AS inventory_health_status
FROM dwh.fct_inventory_daily f
JOIN velocity_7d v ON f.store_key = v.store_key AND f.product_key = v.product_key
JOIN dwh.dim_date d ON f.date_key = d.date_key
JOIN dwh.dim_store s ON f.store_key = s.store_key
JOIN dwh.dim_product p ON f.product_key = p.product_key
Join max_fact_date m on m.max_date = d.full_date
;


-- 4. Forecast deviation
CREATE OR REPLACE VIEW rpt.forecast_deviation AS
SELECT 
    d.full_date,
    s.store_id,
    s.region,
    p.product_id,
	p.category,
    f.units_sold AS actual_sold,
    round(f.demand_forecast) AS forecasted_demand,
    round(f.demand_forecast - f.units_sold) AS deviation_units,
    CASE 
        WHEN f.units_sold = 0 THEN Null 
        ELSE ROUND(ABS(f.demand_forecast - f.units_sold)::numeric / f.units_sold*100,2)
    END AS error_pct,
    CASE 
        WHEN f.units_sold = 0 AND f.demand_forecast = 0 THEN 'ACCURATE'
        WHEN f.units_sold = 0 AND f.demand_forecast > 0 THEN 'OVER-FORECAST'
        WHEN f.demand_forecast = 0 AND f.units_sold > 0 THEN 'UNDER-FORECAST'
        WHEN ((f.demand_forecast - f.units_sold)::DECIMAL / f.units_sold) > 0.20 THEN 'OVER-FORECAST'
        WHEN ((f.demand_forecast - f.units_sold)::DECIMAL / f.units_sold) < -0.20 THEN 'UNDER-FORECAST'
        ELSE 'ACCURATE'
    END AS accuracy_status
FROM dwh.fct_inventory_daily f
JOIN dwh.dim_date d ON f.date_key = d.date_key
JOIN dwh.dim_store s ON f.store_key = s.store_key
JOIN dwh.dim_product p ON f.product_key = p.product_key
;
