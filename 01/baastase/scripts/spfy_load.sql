-- Laeb hosti CSV-faili andmed tabelisse spfy_products psql-i \COPY käsuga.
--\COPY spfy_products (id, title, description, product_category, status, sku) FROM 'C:/Users/Kasutaja/Downloads/andmeinseneeria/product_template.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');

COPY spfy_products (title, description, product_category, status, sku) FROM '/data/product_template_clean.csv' WITH (FORMAT csv, HEADER true, DELIMITER ',', ENCODING 'UTF8');