-- Replace the date with the processing_date shown in the DAG task log.
SELECT processing_date, country_code, COUNT(*) AS order_count,
       SUM(quantity * unit_price) AS gross_value
FROM country_orders
GROUP BY processing_date, country_code
ORDER BY processing_date, country_code;

-- This query should return no rows if the composite key is working.
SELECT processing_date, country_code, order_id, COUNT(*) AS copies
FROM country_orders
GROUP BY processing_date, country_code, order_id
HAVING COUNT(*) > 1;
