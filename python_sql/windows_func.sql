SELECT
    sale_date,
    sale_amount,
    RANK() OVER (PARTITION BY sale_date ORDER BY sale_amount DESC) AS sale_rank
FROM sales_data
WHERE sale_date <> '' and sale_amount <> 0
ORDER BY sale_date, sale_amount DESC;

SELECT
    sale_date,
    sale_amount,
    SUM(sale_amount) OVER (PARTITION BY sale_date) AS total_sale_amount
FROM sales_data
WHERE sale_date <> '' and sale_amount <> 0
ORDER BY sale_date;

SELECT Platform
	, SUM(sale_amount)
	, SUM(request_amount)  
	, ROUND(SUM(sale_amount)/SUM(request_amount)*100,2) Percentage_of_order 
FROM(
  SELECT 
    CASE WHEN tracking LIKE '%Andorid' THEN 'Andorid'
         WHEN tracking LIKE '%Desktop' THEN 'Desktop'
         WHEN tracking LIKE '%iOS' THEN 'iOS'
         WHEN tracking LIKE '%mWeb' THEN 'mWeb'
         ELSE 'Other' END AS Platform
    , sale_amount
	, request_amount
  FROM sales_data
  WHERE sale_amount <> 0
) tracking 
GROUP BY Platform;

SELECT * FROM 
(SELECT sd.lead_id , sd.sale_amount, LAG(sale_amount) OVER (PARTITION BY lead_id ORDER BY "_ingested_at") as lag_sale, sd.id 
FROM sales_data as sd
ORDER BY sd.lead_id, "_ingested_at")as lag_tab
WHERE lag_tab.sale_amount > lag_tab.lag_sale

WITH not_equal_sales AS (
  SELECT id, lag_sale
  FROM (
    SELECT sd.sale_amount, LAG(sale_amount) OVER (PARTITION BY lead_id ORDER BY "_ingested_at") as lag_sale, sd.id 
    FROM sales_data as sd
    ORDER BY sd.lead_id, "_ingested_at"
  ) lag_tab
  WHERE lag_tab.sale_amount > lag_tab.lag_sale
)
SELECT * FROM not_equal_sales;
	