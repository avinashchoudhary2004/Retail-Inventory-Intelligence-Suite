/* ================================================================================
  PURPOSE: Defines the Reporting Layer (RPT schema) for the Retail Inventory Dashboard.
  
  CONTENTS:
  1. view_inventory_daily_snapshot   - One Big Table (OBT) for generic analysis
  2. view_inventory_health_final     - Current stock status & alerts
  3. view_forecast_accuracy_daily    - ML Model performance monitoring
  4. view_inventory_turnover_monthly - Capital efficiency & turnover rates
  5. next_7d_demand                  - Next 7d demand forecast
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
    f.inventory_level,
	f.units_ordered,
    f.demand_forecast,
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

-- 3. Inventory status
CREATE OR REPLACE VIEW rpt.inventory_health AS
WITH max_fact_date AS (
    SELECT MAX(d.full_date) AS max_date
    FROM dwh.fct_inventory_daily f
    JOIN dwh.dim_date d ON f.date_key = d.date_key
), forecasted_demand AS (
	SELECT 
	    d.full_date,
	    s.store_id,
		s.region,
		p.product_id,
	    p.category,
		f.inventory_level,
		f.units_ordered,
	    f.inventory_level + f.units_ordered AS effective_inventory_level,
		n.full_date AS forecasted_for,
        n.demand_forecast,
	    sum(n.demand_forecast) over(partition by s.store_id, p.product_id order by n.full_date) as running_total,
		row_number() over(partition by s.store_id, p.product_id order by n.full_date) as rn,
		CASE
			WHEN f.inventory_level + f.units_ordered < sum(n.demand_forecast) over(partition by s.store_id, p.product_id order by n.full_date)
				THEN TRUE
			ELSE FALSE
		END AS is_less_than_demand
	FROM dwh.fct_inventory_daily f
	JOIN dwh.dim_date d ON f.date_key = d.date_key
	JOIN dwh.dim_store s ON f.store_key = s.store_key
	JOIN dwh.dim_product p ON f.product_key = p.product_key
	JOIN max_fact_date m ON m.max_date = d.full_date
	JOIN dwh.demand_forecast_7d n ON n.store_id = s.store_id AND n.product_id = p.product_id
), DoS AS (
	SELECT
		store_id,
		product_id,
		Round(MIN(
                CASE WHEN rn = 1 THEN ROUND(effective_inventory_level / NULLIF(demand_forecast, 0), 1)
                    WHEN rn = 7 and is_less_than_demand = False THEN 7.0 + ((effective_inventory_level - running_total) / NULLIF(running_total / 7.0, 0))
                    Else ROUND((rn - 1) + greatest(effective_inventory_level - (running_total - demand_forecast), 0)/ NULLIF(demand_forecast, 0), 1)
                END 
            ) 
        , 2) as DoS
	FROM forecasted_demand
	WHERE is_less_than_demand = True OR (rn = 7 AND is_less_than_demand = False)
	GROUP BY 1,2
)
SELECT 
	f.full_date,
	f.store_id,
	f.region,
	f.product_id,
	f.category,
	f.inventory_level,
	f.units_ordered,
	f.inventory_level + f.units_ordered AS effective_inventory,
	d.DoS,
	running_total as next_day_demand,
	CASE
        WHEN running_total > inventory_level AND DoS < 1 THEN 'Critical'
        WHEN running_total > inventory_level AND DoS >= 1 THEN 'Critical'
        WHEN DoS >= 1 AND DoS < 2 THEN 'Low'
        WHEN DoS >= 2 AND DoS <= 4 THEN 'Healthy'
        WHEN DoS > 4 THEN 'Overstocked'
    END AS inventory_health_status,
	CASE
        WHEN running_total > inventory_level AND DoS < 1 THEN 'Order more and delivery by tomorrow'
        WHEN running_total > inventory_level AND DoS >= 1 THEN 'Delivery by tomorrow'
        WHEN DoS >= 1 AND DoS < 2 THEN 'Order more'
        WHEN DoS > 4 AND units_ordered > 0 THEN 'Cancel existing order and apply discount'
        WHEN DoS > 4 AND units_ordered = 0 THEN 'Apply discount'
        ELSE 'No action required'
    END AS inventory_action_required,
	CASE
        WHEN running_total > inventory_level AND DoS < 1 THEN 1
        WHEN running_total > inventory_level AND DoS >= 1 THEN 2
        WHEN DoS >= 1 AND DoS < 2 THEN 3
        WHEN DoS >= 2 AND DoS <= 4 THEN 5
        WHEN DoS > 4 THEN 4
    END AS severity_level
FROM forecasted_demand f
JOIN DoS d ON d.store_id = f.store_id AND d.product_id = f.product_id 
WHERE rn = 1
;

-- 4. Forecast deviation
CREATE OR REPLACE VIEW rpt.forecast_deviation AS
SELECT 
    d.full_date,
    trim(s.store_id) as store_id,
    trim(p.product_id) as product_id,
	trim(p.category) as category,
    f.units_sold,
    round(f.demand_forecast,2) AS demand_forecast,
    round(f.demand_forecast - f.units_sold, 2) AS units_deviation,
    f.selling_price as price,
    f.discount_applied as discount,
    f.is_promotion_active,
    f.weather_condition,
    d.season as seaonality,
    f.competitor_price,
	trim(
        CASE 
            WHEN f.units_sold = 0 AND f.demand_forecast = 0 THEN 'accurate'
            WHEN f.units_sold = 0 AND f.demand_forecast > 0 THEN 'over-forecast'
            WHEN f.demand_forecast = 0 AND f.units_sold > 0 THEN 'under-forecast'
            WHEN ((f.demand_forecast - f.units_sold)::DECIMAL / f.units_sold) > 0.10 THEN 'over-forecast'
            WHEN ((f.demand_forecast - f.units_sold)::DECIMAL / f.units_sold) < -0.10 THEN 'under-forecast'
            ELSE 'accurate'
        END 
    ) AS forecast_accuracy_flag
FROM dwh.fct_inventory_daily f
JOIN dwh.dim_date d ON f.date_key = d.date_key
JOIN dwh.dim_store s ON f.store_key = s.store_key
JOIN dwh.dim_product p ON f.product_key = p.product_key
ORDER BY full_date, store_id, product_id ASC
;

-- 5. Next 7d forecast
SELECT 
    forecasted_on,
    full_date,
    trim(store_id) as store_id,
    trim(product_id) as product_id,
    trim(category) as category,
	trim(region) as region,
    selling_price,
    discount_applied,
    weather_condition,
    is_promotion_active,
    competitor_price,
    season,
    demand_forecast
FROM dwh.demand_forecast_7d
ORDER BY full_date, store_id, product_id ASC
;
