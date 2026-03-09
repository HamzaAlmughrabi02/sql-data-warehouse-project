/*
===============================================================================
Data Quality Checks - Silver Layer
===============================================================================
Purpose:
    To validate the integrity, consistency, and standardization of data 
    within the Silver layer after the ETL process.
    
Expectation: 
    Most queries should return "No Result" or empty sets if the cleaning 
    logic in 'load_silver' is working correctly.
===============================================================================
*/

-------------------------------------------------------------------------------
-- 1) CRM Tables Checks
-------------------------------------------------------------------------------

-- >> Table: silver.crm_prd_info
PRINT 'Checking Quality for: silver.crm_prd_info';

-- Check for Duplicates or Nulls in Primary Key
SELECT prd_id, COUNT(*) 
FROM silver.crm_prd_info 
GROUP BY prd_id 
HAVING COUNT(*) > 1 OR prd_id IS NULL;

-- Check for Unwanted Spaces in Product Name
SELECT prd_nm FROM silver.crm_prd_info 
WHERE prd_nm != TRIM(prd_nm);

-- Check for Out-of-Range or Null Costs
SELECT prd_cost FROM silver.crm_prd_info 
WHERE prd_cost < 0 OR prd_cost IS NULL;

-- Check Standardization of Product Line
SELECT DISTINCT prd_line FROM silver.crm_prd_info;

-- Check for Invalid Date Logic (Start Date must be before End Date)
SELECT * FROM silver.crm_prd_info 
WHERE prd_start_date > prd_end_date;

-- ----------------------------------------------------------------------------

-- >> Table: silver.crm_cust_info
PRINT 'Checking Quality for: silver.crm_cust_info';

-- Check for Duplicates or Nulls in Primary Key
SELECT cst_id, COUNT(*) 
FROM silver.crm_cust_info 
GROUP BY cst_id 
HAVING COUNT(*) > 1 OR cst_id IS NULL;

-- Check for Unwanted Spaces in Names
SELECT cst_firstname FROM silver.crm_cust_info WHERE cst_firstname != TRIM(cst_firstname);
SELECT cst_lastname FROM silver.crm_cust_info WHERE cst_lastname != TRIM(cst_lastname);

-- Check Standardization of Gender and Marital Status
SELECT DISTINCT cst_gndr FROM silver.crm_cust_info;
SELECT DISTINCT cst_marital_status FROM silver.crm_cust_info;

-- ----------------------------------------------------------------------------

-- >> Table: silver.crm_sls_details
PRINT 'Checking Quality for: silver.crm_sls_details';

-- Check for Out-of-Range Dates
SELECT * FROM silver.crm_sls_details 
WHERE sls_order_dt > '2050-01-01' OR sls_order_dt < '1900-01-01';

SELECT * FROM silver.crm_sls_details 
WHERE sls_ship_dt > '2050-01-01' OR sls_ship_dt < '1900-01-01';

SELECT * FROM silver.crm_sls_details 
WHERE sls_due_dt > '2050-01-01' OR sls_due_dt < '1900-01-01';

-- Check for Invalid Date Sequence
SELECT * FROM silver.crm_sls_details 
WHERE sls_order_dt > sls_ship_dt OR sls_order_dt > sls_due_dt;

-- Check Data Consistency: Sales, Quantity, and Price
-- Expectation: sls_sales = sls_quantity * sls_price
SELECT 
    sls_ord_num,
    sls_quantity,
    sls_sales,
    sls_price,
    (sls_quantity * sls_price) as calculated_sales
FROM silver.crm_sls_details
WHERE ABS(sls_sales - (sls_quantity * sls_price)) > 0.01 -- Check for calculation errors
   OR sls_sales <= 0 
   OR sls_quantity <= 0 
   OR sls_price <= 0
   OR sls_sales IS NULL;

-------------------------------------------------------------------------------
-- 2) ERP Tables Checks
-------------------------------------------------------------------------------

-- >> Table: silver.erp_cust_az12
PRINT 'Checking Quality for: silver.erp_cust_az12';

-- Identify Out-of-Range Birth Dates
SELECT DISTINCT cst_bdate FROM silver.erp_cust_az12 
WHERE cst_bdate < '1924-01-01' OR cst_bdate > GETDATE();

-- Check Standardization of Gender
SELECT DISTINCT cst_gen FROM silver.erp_cust_az12;

-- ----------------------------------------------------------------------------

-- >> Table: silver.erp_loc_a101
PRINT 'Checking Quality for: silver.erp_loc_a101';

-- Check Standardization of Country Names
SELECT DISTINCT loc_cntry FROM silver.erp_loc_a101 
ORDER BY loc_cntry;

-- ----------------------------------------------------------------------------

-- >> Table: silver.erp_px_cat_g1v2
PRINT 'Checking Quality for: silver.erp_px_cat_g1v2';

-- Check for Unwanted Spaces in Categories
SELECT px_cat, px_subcat FROM silver.erp_px_cat_g1v2 
WHERE px_cat != TRIM(px_cat) 
   OR px_subcat != TRIM(px_subcat);

-- Check Standardization of Maintenance Flag
SELECT DISTINCT px_maintenance FROM silver.erp_px_cat_g1v2;
