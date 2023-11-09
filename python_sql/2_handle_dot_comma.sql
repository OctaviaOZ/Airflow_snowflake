UPDATE sales_data 
SET request_amount = REPLACE(request_amount, ',', '.')
WHERE request_amount LIKE '%,%';

UPDATE sales_data 
SET sale_amount = REPLACE(sale_amount, ',', '.')
WHERE sale_amount LIKE '%,%';