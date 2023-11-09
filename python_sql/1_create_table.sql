CREATE TABLE sales_data (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    _ingested_at TIMESTAMP,
    lead_id VARCHAR(50),
    at_date DATE,
    request_amount NUMERIC(10, 2),
    sale INTEGER,
    sale_amount NUMERIC(10, 2),
    sale_date DATE,
    lead_beendet__kredit_storniert INTEGER,
    tracking VARCHAR(50)
);