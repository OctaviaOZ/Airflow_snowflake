SELECT * FROM dummy
WHERE LOWER(name) <> LOWER(SUBSTR(email, 1, INSTR(email, '@') - 1));