-- Handling missing values (setting NaN values to NULL)
UPDATE sales_data SET sale_amount = NULL WHERE sale_amount = 'nan';