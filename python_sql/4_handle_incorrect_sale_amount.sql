UPDATE sales_data 
SET sale_amount = request_amount
WHERE ROUND(request_amount * 100) = sale_amount;

UPDATE sales_data AS update_sd
SET sale_amount = (
  SELECT lag_sale
  FROM (
    SELECT sd.sale_amount, LAG(sale_amount) OVER (PARTITION BY lead_id ORDER BY "_ingested_at") AS lag_sale, sd.id 
    FROM sales_data AS sd
  ) lag_tab
  WHERE lag_tab.sale_amount > lag_tab.lag_sale
  AND update_sd.id = lag_tab.id
)
WHERE update_sd.id in (
	SELECT id
	FROM (
    	SELECT sd.sale_amount, LAG(sale_amount) OVER (PARTITION BY lead_id ORDER BY "_ingested_at") AS lag_sale, sd.id 
    	FROM sales_data AS sd
  	) lag_tab
  	WHERE lag_tab.sale_amount > lag_tab.lag_sale);