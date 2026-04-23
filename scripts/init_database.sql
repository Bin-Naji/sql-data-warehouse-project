/*
=============================================================
Create Database and Schemas
=============================================================
Script Purpose:
    This script creates a new database named 'datawarehouse' 
    after checking if it already exists. If the database exists, 
    it is dropped and recreated. Additionally, the script sets up 
    three schemas within the database: 'bronze', 'silver', and 'gold'.

WARNING:
    Running this script will drop the entire 'datawarehouse' database 
    if it exists. All data in the database will be permanently deleted. 
    Proceed with caution and ensure you have proper backups before 
    running this script.

INSTRUCTIONS:
    Step 1 - Run the block below while connected to the default 
             'postgres' database.
    Step 2 - Reconnect to 'datawarehouse' then run the schema block.
             In pgAdmin: right-click the database > Connect
             In psql: \c datawarehouse
=============================================================
*/

-- =============================================================
-- Step 1: Run while connected to the 'postgres' database
-- =============================================================

-- Terminate any active connections to the target database
SELECT pg_terminate_backend(pid)
FROM pg_stat_activity
WHERE datname = 'datawarehouse' AND pid <> pg_backend_pid();

-- Drop the existing database if it exists
DROP DATABASE IF EXISTS datawarehouse;

-- Create the 'datawarehouse' database
CREATE DATABASE datawarehouse;


-- =============================================================
-- Step 2: Reconnect to 'datawarehouse' then run below
-- =============================================================

-- Create Schemas
CREATE SCHEMA IF NOT EXISTS bronze;
CREATE SCHEMA IF NOT EXISTS silver;
CREATE SCHEMA IF NOT EXISTS gold;
