# DataSync - Multi-Database Synchronization Tool

## How to Control Table Synchronization

DataSync only processes tables that have `active=true` in the `metadata.catalog` table.

### Enable/Disable Table Synchronization

```sql
-- Enable table synchronization
UPDATE metadata.catalog SET active=true WHERE schema_name='your_schema' AND table_name='your_table';

-- Disable table synchronization
UPDATE metadata.catalog SET active=false WHERE schema_name='your_schema' AND table_name='your_table';

-- Check current table status
SELECT schema_name, table_name, db_engine, active, status FROM metadata.catalog;
```

### Supported Synchronization Types

- **MariaDB → PostgreSQL**: `WHERE active=true AND db_engine='MariaDB'`
- **PostgreSQL → MariaDB**: `WHERE active=true AND db_engine='Postgres' AND replicate_to_mariadb=true`

### Quick Commands

```sql
-- Reset a table to full_load (re-sync all data)
UPDATE metadata.catalog SET status='full_load', last_offset=0 WHERE schema_name='schema' AND table_name='table';

-- Pause synchronization for a table
UPDATE metadata.catalog SET active=false WHERE schema_name='schema' AND table_name='table';
```

cmake . && make

sudo mkdir -p /opt/datasync_temp && sudo chmod 777 /opt/datasync_temp
