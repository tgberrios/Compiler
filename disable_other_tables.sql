-- Desactivar todas las tablas excepto las de PostgreSQL que van a MariaDB
-- Solo mantener activas las tablas con db_engine='Postgres' AND replicate_to_mariadb=true

-- Desactivar todas las tablas de MariaDB
UPDATE metadata.catalog 
SET active = false 
WHERE db_engine = 'MariaDB';

-- Desactivar todas las tablas de MSSQL
UPDATE metadata.catalog 
SET active = false 
WHERE db_engine = 'MSSQL';

-- Desactivar las tablas de PostgreSQL que NO van a MariaDB
UPDATE metadata.catalog 
SET active = false 
WHERE db_engine = 'Postgres' AND replicate_to_mariadb = false;

-- Verificar el resultado
SELECT schema_name, table_name, db_engine, active, replicate_to_mariadb, replicate_to_mssql 
FROM metadata.catalog 
WHERE active = true
ORDER BY db_engine, schema_name, table_name;
