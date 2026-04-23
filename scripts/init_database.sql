## 🗄️ Database Setup

This script creates a new database named `datawarehouse` and sets up three schemas: `bronze`, `silver`, and `gold`.

> ⚠️ **Warning:** Running this script will permanently drop the existing `datawarehouse` database if it exists. Ensure you have proper backups before proceeding.

### Step 1 – Run in your default `postgres` database

```sql
-- Terminate active connections to the database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse' AND pid <> pg_backend_pid();

-- Drop the existing database if it exists
DROP DATABASE IF EXISTS datawarehouse;

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;
```

### Step 2 – Connect to `datawarehouse` then run

```sql
-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
```

> 💡 **Note:** In pgAdmin, reconnect by right-clicking the database and selecting **Connect**. In psql, switch using `\c datawarehouse`.
