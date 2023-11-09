WITH filtered_sales_data
AS
(
    SELECT DISTINCT "_ingested_at"
    	, sd_f.lead_id
    	, at_date
    	, request_amount
    	, sale
    	, sale_amount
    	, sale_date
    	, lead_beendet__kredit_storniert
    	, tracking
    FROM sales_data AS sd
    INNER JOIN (
        SELECT lead_id, MAX(_ingested_at) AS max_timestamp
        FROM sales_data
        GROUP BY lead_id
    ) sd_f ON sd.lead_id = sd_f.lead_id AND sd._ingested_at = sd_f.max_timestamp
)

SELECT sum(request_amount), sum(sale_amount), count(*)
FROM filtered_sales_data;