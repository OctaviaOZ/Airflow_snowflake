CREATE TABLE dummy (id INTEGER PRIMARY KEY, name VARCHAR, class INTEGER, email VARCHAR);
INSERT INTO dummy_db.main.dummy 
SELECT * FROM
read_json_auto("C:\GitHub\technical_challenge\python_sql\dummy.json");

SELECT * FROM dummy_db.main.dummy 