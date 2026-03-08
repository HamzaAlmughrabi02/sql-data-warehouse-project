/*
-------------------------------------------------------------------------------
Stored Procedure for Loading
-------------------------------------------------------------------------------
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files.
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the BULK INSERT command to load data from csv Files to bronze tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
*/



CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME;
    DECLARE @rows_inserted INT;

    BEGIN TRY
        SET @batch_start_time = GETDATE();
        
        PRINT '================================================';
        PRINT 'Loading Bronze Layer';
        PRINT 'Start Time: ' + CAST(@batch_start_time AS NVARCHAR);
        PRINT '================================================';

        -----------------------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading CRM Tables';
        PRINT '------------------------------------------------';

        -- Table: bronze.crm_cust_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;
        PRINT '>> Inserting Data From: cust_info.csv';
        BULK INSERT bronze.crm_cust_info
        FROM 'C:\Users\halmu\OneDrive\Desktop\mapdata\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   [OK] Rows: ' + CAST(@rows_inserted AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
		    PRINT '. . . . . . . . . . . . . . . . . . . . . '
        -- Table: bronze.crm_prd_info
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;
        PRINT '>> Inserting Data From: prd_info.csv';
        BULK INSERT bronze.crm_prd_info
        FROM 'C:\Users\halmu\OneDrive\Desktop\mapdata\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   [OK] Rows: ' + CAST(@rows_inserted AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
		    PRINT '. . . . . . . . . . . . . . . . . . . . . '

        -- Table: bronze.crm_sls_details
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.crm_sls_details';
        TRUNCATE TABLE bronze.crm_sls_details;
        PRINT '>> Inserting Data From: sales_details.csv';
        BULK INSERT bronze.crm_sls_details
        FROM 'C:\Users\halmu\OneDrive\Desktop\mapdata\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   [OK] Rows: ' + CAST(@rows_inserted AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
		    PRINT '. . . . . . . . . . . . . . . . . . . . . '

        -----------------------------------------------------------------------
        PRINT '------------------------------------------------';
        PRINT 'Loading ERP Tables';
        PRINT '------------------------------------------------';

        -- Table: bronze.erp_cust_az12
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;
        PRINT '>> Inserting Data From: CUST_AZ12.csv';
        BULK INSERT bronze.erp_cust_az12
        FROM 'C:\Users\halmu\OneDrive\Desktop\mapdata\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   [OK] Rows: ' + CAST(@rows_inserted AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
		    PRINT '. . . . . . . . . . . . . . . . . . . . . '

        -- Table: bronze.erp_loc_a101
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;
        PRINT '>> Inserting Data From: LOC_A101.csv';
        BULK INSERT bronze.erp_loc_a101
        FROM 'C:\Users\halmu\OneDrive\Desktop\mapdata\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   [OK] Rows: ' + CAST(@rows_inserted AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
		    PRINT '. . . . . . . . . . . . . . . . . . . . . '

        -- Table: bronze.erp_px_cat_g1v2
        SET @start_time = GETDATE();
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;
        PRINT '>> Inserting Data From: PX_CAT_G1V2.csv';
        BULK INSERT bronze.erp_px_cat_g1v2
        FROM 'C:\Users\halmu\OneDrive\Desktop\mapdata\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
        WITH (FIRSTROW = 2, FIELDTERMINATOR = ',', ROWTERMINATOR = '\n', TABLOCK);
        SET @rows_inserted = @@ROWCOUNT;
        SET @end_time = GETDATE();
        PRINT '   [OK] Rows: ' + CAST(@rows_inserted AS NVARCHAR) + ' | Duration: ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 's';
		    PRINT '. . . . . . . . . . . . . . . . . . . . . '

        -----------------------------------------------------------------------
        PRINT '================================================';
        PRINT 'Batch Loading Completed Successfully';
        PRINT 'Total Duration: ' + CAST(DATEDIFF(second, @batch_start_time, GETDATE()) AS NVARCHAR) + ' seconds';
        PRINT '================================================';

    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'CRITICAL ERROR DURING BATCH LOAD';
        PRINT 'Error Message: ' + ERROR_MESSAGE();
        PRINT 'Error Number: ' + CAST (ERROR_NUMBER() AS NVARCHAR);
        PRINT '================================================';
    END CATCH
END;
