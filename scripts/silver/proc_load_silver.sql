CREATE OR REPLACE PROCEDURE silver.load_silver()
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
    RAISE NOTICE 'Loading Silver Layer';
    RAISE NOTICE '=============================================';
    total_start_time := clock_timestamp();

    -- Load crm_cust_info
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.crm_cust_info';
    RAISE NOTICE '---------------------------------------------';
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE silver.crm_cust_info;
        
        INSERT INTO silver.crm_cust_info (
            cst_id, cst_key, cst_firstname, cst_lastname, 
            cst_marital_status, cst_gndr, cst_create_date
        )
        SELECT
            cst_id,
            cst_key,
            TRIM(cst_firstname) AS cst_firstname,
            TRIM(cst_lastname) AS cst_lastname,
            CASE 
                WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
                WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
                ELSE 'n/a'
            END AS cst_marital_status,
            CASE 
                WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
                WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
                ELSE 'n/a'
            END AS cst_gndr,
            cst_create_date::DATE
        FROM (
            SELECT *, 
                ROW_NUMBER() OVER (
                    PARTITION BY cst_id 
                    ORDER BY cst_create_date DESC
                ) AS flag_last
            FROM bronze.crm_cust_info
            WHERE cst_id IS NOT NULL
        ) AS t
        WHERE flag_last = 1;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded % rows in % seconds', 
            (SELECT COUNT(*) FROM silver.crm_cust_info), 
            EXTRACT(EPOCH FROM duration);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading crm_cust_info: %', SQLERRM;
    END;

    -- Load crm_prd_info
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.crm_prd_info';
    RAISE NOTICE '---------------------------------------------';
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE silver.crm_prd_info;
        
        INSERT INTO silver.crm_prd_info (
            prd_id, cat_id, prd_key, prd_nm, 
            prd_cost, prd_line, prd_start_dt, prd_end_dt
        )
        SELECT
            prd_id,
            REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
            SUBSTRING(prd_key, 7) AS prd_key,
            prd_nm,
            COALESCE(prd_cost, 0) AS prd_cost,
            CASE UPPER(TRIM(prd_line))
                WHEN 'M' THEN 'Mountain'
                WHEN 'R' THEN 'Road'
                WHEN 'S' THEN 'Other Sales'
                WHEN 'T' THEN 'Touring'
                ELSE 'n/a'
            END AS prd_line,
            prd_start_dt::DATE,
            (LEAD(prd_start_dt) OVER (
                PARTITION BY prd_key 
                ORDER BY prd_start_dt
            ) - '1 day'::INTERVAL)::DATE AS prd_end_dt
        FROM bronze.crm_prd_info;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded % rows in % seconds', 
            (SELECT COUNT(*) FROM silver.crm_prd_info), 
            EXTRACT(EPOCH FROM duration);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading crm_prd_info: %', SQLERRM;
    END;

    -- Load crm_sales_details
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.crm_sales_details';
    RAISE NOTICE '---------------------------------------------';
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE silver.crm_sales_details;
        
        INSERT INTO silver.crm_sales_details (
            sls_ord_num, sls_prd_key, sls_cust_id, sls_order_dt,
            sls_ship_dt, sls_due_dt, sls_sales, sls_quantity, sls_price
        )
        SELECT
            sls_ord_num,
            sls_prd_key,
            sls_cust_id,
            CASE
                WHEN CAST(sls_order_dt AS TEXT) ~ '^\d{8}$' THEN TO_DATE(CAST(sls_order_dt AS TEXT), 'YYYYMMDD')
                WHEN CAST(sls_order_dt AS TEXT) ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(sls_order_dt AS TEXT)::DATE
                ELSE NULL
            END AS sls_order_dt,
            CASE
                WHEN CAST(sls_ship_dt AS TEXT) ~ '^\d{8}$' THEN TO_DATE(CAST(sls_ship_dt AS TEXT), 'YYYYMMDD')
                WHEN CAST(sls_ship_dt AS TEXT) ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(sls_ship_dt AS TEXT)::DATE
                ELSE NULL
            END AS sls_ship_dt,
            CASE
                WHEN CAST(sls_due_dt AS TEXT) ~ '^\d{8}$' THEN TO_DATE(CAST(sls_due_dt AS TEXT), 'YYYYMMDD')
                WHEN CAST(sls_due_dt AS TEXT) ~ '^\d{4}-\d{2}-\d{2}$' THEN CAST(sls_due_dt AS TEXT)::DATE
                ELSE NULL
            END AS sls_due_dt,
            CASE
                WHEN sls_sales IS NULL OR sls_sales <= 0 
                     OR sls_sales != sls_quantity * ABS(sls_price) 
                THEN sls_quantity * ABS(sls_price)
                ELSE sls_sales
            END AS sls_sales,
            sls_quantity,
            CASE
                WHEN sls_price IS NULL OR sls_price <= 0 
                THEN sls_sales / NULLIF(sls_quantity, 0)
                ELSE sls_price
            END AS sls_price
        FROM bronze.crm_sales_details;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded % rows in % seconds', 
            (SELECT COUNT(*) FROM silver.crm_sales_details), 
            EXTRACT(EPOCH FROM duration);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading crm_sales_details: %', SQLERRM;
    END;

    -- Load erp_cust_az12
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.erp_cust_az12';
    RAISE NOTICE '---------------------------------------------';
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE silver.erp_cust_az12;
        
        INSERT INTO silver.erp_cust_az12 (
            cid,
            bdate,
            gen
        )
        SELECT
            CASE
                WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid, 4)
                ELSE cid
            END AS cid,
            CASE
                WHEN bdate > CURRENT_DATE THEN NULL
                ELSE bdate::DATE
            END AS bdate,
            CASE
                WHEN UPPER(TRIM(gen)) IN ('F', 'FEMALE') THEN 'Female'
                WHEN UPPER(TRIM(gen)) IN ('M', 'MALE') THEN 'Male'
                ELSE 'n/a'
            END AS gen
        FROM bronze.erp_cust_az12;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded % rows in % seconds', 
            (SELECT COUNT(*) FROM silver.erp_cust_az12), 
            EXTRACT(EPOCH FROM duration);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading erp_cust_az12: %', SQLERRM;
    END;

    -- Load erp_loc_a101
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.erp_loc_a101';
    RAISE NOTICE '---------------------------------------------';
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE silver.erp_loc_a101;
        
        INSERT INTO silver.erp_loc_a101 (
            cid,
            cntry
        )
        SELECT
            REPLACE(cid, '-', '') AS cid,
            CASE
                WHEN TRIM(cntry) = 'DE' THEN 'Germany'
                WHEN TRIM(cntry) IN ('US', 'USA') THEN 'United States'
                WHEN TRIM(cntry) = '' OR cntry IS NULL THEN 'n/a'
                ELSE INITCAP(LOWER(TRIM(cntry)))
            END AS cntry
        FROM bronze.erp_loc_a101;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded % rows in % seconds', 
            (SELECT COUNT(*) FROM silver.erp_loc_a101), 
            EXTRACT(EPOCH FROM duration);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading erp_loc_a101: %', SQLERRM;
    END;

    -- Load erp_px_cat_g1v2
    RAISE NOTICE '---------------------------------------------';
    RAISE NOTICE 'Processing Table: silver.erp_px_cat_g1v2';
    RAISE NOTICE '---------------------------------------------';
    BEGIN
        start_time := clock_timestamp();
        TRUNCATE TABLE silver.erp_px_cat_g1v2;
        
        INSERT INTO silver.erp_px_cat_g1v2 (
            id,
            cat,
            subcat,
            maintenance
        )
        SELECT
            id,
            TRIM(cat) AS cat,
            TRIM(subcat) AS subcat,
            CASE
                WHEN maintenance = 'Y' THEN 'Yes'
                WHEN maintenance = 'N' THEN 'No'
                ELSE 'Unknown'
            END AS maintenance
        FROM bronze.erp_px_cat_g1v2;

        end_time := clock_timestamp();
        duration := end_time - start_time;
        RAISE NOTICE 'Loaded % rows in % seconds', 
            (SELECT COUNT(*) FROM silver.erp_px_cat_g1v2), 
            EXTRACT(EPOCH FROM duration);
    EXCEPTION
        WHEN OTHERS THEN
            RAISE EXCEPTION 'Error loading erp_px_cat_g1v2: %', SQLERRM;
    END;

    total_end_time := clock_timestamp();
    total_duration := total_end_time - total_start_time;
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Total Silver Processing Duration: % seconds', 
        EXTRACT(EPOCH FROM total_duration);
    RAISE NOTICE '=============================================';
    RAISE NOTICE 'Silver Layer Loaded Successfully';
    RAISE NOTICE '=============================================';
EXCEPTION
    WHEN OTHERS THEN
        RAISE EXCEPTION 'Silver layer load failed: %', SQLERRM;
END;
$$;

CALL silver.load_silver();