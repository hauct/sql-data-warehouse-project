-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT
    cst_id,
    COUNT(*)
FROM
    silver.crm_cust_info
GROUP BY
    cst_id
HAVING
    COUNT(*) > 1 OR cst_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Results

SELECT
    cst_key
FROM
    silver.crm_cust_info
WHERE
    cst_key != TRIM(cst_key);

-- Data Standardization & Consistency
SELECT DISTINCT
    cst_marital_status
FROM
    silver.crm_cust_info;

-- =====================================
-- Check For Nulls or Duplicates in Primary Key
-- Expectation: No Result

SELECT
    prd_id,
    COUNT(*)
FROM
    silver.crm_prd_info
GROUP BY
    prd_id
HAVING
    COUNT(*) > 1 OR prd_id IS NULL;

-- Check for unwanted Spaces
-- Expectation: No Results

SELECT
    prd_nm
FROM
    silver.crm_prd_info
WHERE
    prd_nm != TRIM(prd_nm);

-- Check for NULLs or Negative Numbers
-- Expectation: No Results

SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Data Standardization & Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info;

-- Check for Invalid Date Orders
SELECT *
FROM silver.crm_prd_info
WHERE prd_end_dt < prd_start_dt;

-- ================
-- Identify Out-of-Range Dates
SELECT DISTINCT
    bdate
FROM
    silver.erp_cust_az12
WHERE
    bdate < '1924-01-01' OR bdate > CURRENT_DATE;

-- Data Standardization & Consistency
SELECT DISTINCT
    gen
FROM
    silver.erp_cust_az12;

-- =====================
-- Data Standardization & Consistency
SELECT DISTINCT cntry
FROM silver.erp_loc_a101
ORDER BY cntry;

-- ====================
