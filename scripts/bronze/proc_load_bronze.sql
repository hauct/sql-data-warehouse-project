CREATE OR REPLACE PROCEDURE bronze.load_bronze()
LANGUAGE plpgsql
AS $$
DECLARE
	start_time TIMESTAMP;
	end_time TIMESTAMP;
	duration INTERVAL;
	total_start_time TIMESTAMP;
	total_end_time TIMESTAMP;
	total_duration INTERVAL;
BEGIN
	RAISE NOTICE '=============================================';
	RAISE NOTICE 'Loading Bronze Layer';
	RAISE NOTICE '=============================================';
	total_start_time := clock_timestamp();
	
	RAISE NOTICE '---------------------------------------------';
	RAISE NOTICE 'Loading CRM Tables';
	RAISE NOTICE '---------------------------------------------';
	
	RAISE NOTICE '>>> Truncating table: bronze.crm_cust_info';
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.crm_cust_info;
	COPY bronze.crm_cust_info
	FROM '/datasets/source_crm/cust_info.csv'
	WITH (FORMAT CSV, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
	duration := end_time - start_time;
	RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM duration);
	
	RAISE NOTICE '>>> Truncating table: bronze.crm_prd_info';
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.crm_prd_info;
	COPY bronze.crm_prd_info
	FROM '/datasets/source_crm/prd_info.csv'
	WITH (FORMAT CSV, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
	duration := end_time - start_time;
	RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM duration);
	
	RAISE NOTICE '>>> Truncating table: bronze.crm_sales_details';
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.crm_sales_details;
	COPY bronze.crm_sales_details
	FROM '/datasets/source_crm/sales_details.csv'
	WITH (FORMAT CSV, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
	duration := end_time - start_time;
	RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM duration);
	
	RAISE NOTICE '---------------------------------------------';
	RAISE NOTICE 'Loading ERP Tables';
	RAISE NOTICE '---------------------------------------------';
	
	RAISE NOTICE '>>> Truncating table: bronze.erp_cust_az12';
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.erp_cust_az12;
	COPY bronze.erp_cust_az12
	FROM '/datasets/source_erp/CUST_AZ12.csv'
	WITH (FORMAT CSV, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
	duration := end_time - start_time;
	RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM duration);
	
	RAISE NOTICE '>>> Truncating table: bronze.erp_loc_a101';
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.erp_loc_a101;
	COPY bronze.erp_loc_a101
	FROM '/datasets/source_erp/LOC_A101.csv'
	WITH (FORMAT CSV, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
	duration := end_time - start_time;
	RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM duration);
	
	RAISE NOTICE '>>> Truncating table: bronze.erp_px_cat_g1v2';
	start_time := clock_timestamp();
	TRUNCATE TABLE bronze.erp_px_cat_g1v2;
	COPY bronze.erp_px_cat_g1v2
	FROM '/datasets/source_erp/PX_CAT_G1V2.csv'
	WITH (FORMAT CSV, HEADER, DELIMITER ',');
	end_time := clock_timestamp();
	duration := end_time - start_time;
	RAISE NOTICE '>>> Load Duration: % seconds', EXTRACT(EPOCH FROM duration);

	total_end_time := clock_timestamp();
	total_duration := total_end_time - total_start_time;
	RAISE NOTICE '=============================================';
	RAISE NOTICE 'Total Ingestion Duration: % seconds', EXTRACT(EPOCH FROM total_duration);
	
	RAISE NOTICE '=============================================';
	RAISE NOTICE 'Bronze Layer Loaded Successfully';
	RAISE NOTICE '=============================================';
EXCEPTION
	WHEN OTHERS THEN
		RAISE EXCEPTION 'An error occurred during load: %', SQLERRM;
END;
$$;

CALL bronze.load_bronze();

