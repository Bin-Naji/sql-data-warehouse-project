/*
=============================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
=============================================================
Script Purpose:
    This stored procedure loads data into the bronze schema
    from external CSV source files (CRM and ERP systems).
    It truncates existing tables before loading fresh data
    and tracks execution time for each table load.

WARNING:
    This procedure truncates all bronze tables before loading.
    All existing data in the bronze layer will be permanently
    deleted. Ensure backups exist before running.

USAGE:
    CALL bronze.load_bronze();
=============================================================
*/

CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time  TIMESTAMP;
	end_time    TIMESTAMP;
	batch_start TIMESTAMP;
	batch_end   TIMESTAMP;
BEGIN
	batch_start := clock_timestamp();

	RAISE NOTICE '==========================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '==========================================';

	-- -----------------------------------------------------
	-- CRM Tables
	-- -----------------------------------------------------
	RAISE NOTICE '------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '------------------------------------------';

	RAISE NOTICE 'Truncating Table: bronze.crm_cust_info';
	TRUNCATE TABLE bronze.crm_cust_info;
	RAISE NOTICE 'Inserting Data Into: bronze.crm_cust_info';
	start_time := clock_timestamp();
	COPY bronze.crm_cust_info
	FROM '/Users/arkanazzani/Downloads/sql-data-warehouse-project/datasets/source_crm/cust_info.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
	RAISE NOTICE '>> bronze.crm_cust_info loaded in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE 'Truncating Table: bronze.crm_sales_details';
	TRUNCATE TABLE bronze.crm_sales_details;
	RAISE NOTICE 'Inserting Data Into: bronze.crm_sales_details';
	start_time := clock_timestamp();
	COPY bronze.crm_sales_details
	FROM '/Users/arkanazzani/Downloads/sql-data-warehouse-project/datasets/source_crm/sales_details.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
	RAISE NOTICE '>> bronze.crm_sales_details loaded in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE 'Truncating Table: bronze.crm_prd_info';
	TRUNCATE TABLE bronze.crm_prd_info;
	RAISE NOTICE 'Inserting Data Into: bronze.crm_prd_info';
	start_time := clock_timestamp();
	COPY bronze.crm_prd_info
	FROM '/Users/arkanazzani/Downloads/sql-data-warehouse-project/datasets/source_crm/prd_info.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
	RAISE NOTICE '>> bronze.crm_prd_info loaded in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	-- -----------------------------------------------------
	-- ERP Tables
	-- -----------------------------------------------------
	RAISE NOTICE '------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '------------------------------------------';

	RAISE NOTICE 'Truncating Table: bronze.erp_cust_az12';
	TRUNCATE TABLE bronze.erp_cust_az12;
	RAISE NOTICE 'Inserting Data Into: bronze.erp_cust_az12';
	start_time := clock_timestamp();
	COPY bronze.erp_cust_az12
	FROM '/Users/arkanazzani/Downloads/sql-data-warehouse-project/datasets/source_erp/CUST_AZ12.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
	RAISE NOTICE '>> bronze.erp_cust_az12 loaded in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE 'Truncating Table: bronze.erp_loc_a101';
	TRUNCATE TABLE bronze.erp_loc_a101;
	RAISE NOTICE 'Inserting Data Into: bronze.erp_loc_a101';
	start_time := clock_timestamp();
	COPY bronze.erp_loc_a101
	FROM '/Users/arkanazzani/Downloads/sql-data-warehouse-project/datasets/source_erp/LOC_A101.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
	RAISE NOTICE '>> bronze.erp_loc_a101 loaded in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	RAISE NOTICE 'Truncating Table: bronze.erp_px_cat_g1v2';
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	RAISE NOTICE 'Inserting Data Into: bronze.erp_px_cat_g1v2';
	start_time := clock_timestamp();
	COPY bronze.erp_px_cat_g1v2
	FROM '/Users/arkanazzani/Downloads/sql-data-warehouse-project/datasets/source_erp/PX_CAT_G1V2.csv'
	WITH (FORMAT csv, HEADER true, DELIMITER ',');
	end_time := clock_timestamp();
	RAISE NOTICE '>> bronze.erp_px_cat_g1v2 loaded in: % seconds', EXTRACT(EPOCH FROM (end_time - start_time));

	-- -------------------------------------------------
	-- Total Batch Time
	-- -------------------------------------------------
	batch_end := clock_timestamp();
	RAISE NOTICE '======================================';
	RAISE NOTICE 'Bronze Layer Loading Complete';
	RAISE NOTICE 'Total Execution Time: % seconds', EXTRACT(EPOCH FROM (batch_end - batch_start));
	RAISE NOTICE '======================================';

EXCEPTION
	WHEN OTHERS THEN
		RAISE NOTICE '===============================================';
		RAISE NOTICE 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		RAISE NOTICE 'Error Message: %', SQLERRM;
		RAISE NOTICE 'Error Code:    %', SQLSTATE;
		RAISE NOTICE '===============================================';

END;
$$;
