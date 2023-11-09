-- Example: Export query results to a CSV file
COPY (SELECT * FROM sales_data WHERE sale = 1) TO '/path/to/output.csv' CSV HEADER;
