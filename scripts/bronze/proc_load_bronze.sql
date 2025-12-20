/*
===============================================================================
Stored Procedure: Load Bronze Layer (Source -> Bronze)
===============================================================================
Script Purpose:
    This stored procedure loads data into the 'bronze' schema from external CSV files. 
    It performs the following actions:
    - Truncates the bronze tables before loading data.
    - Uses the `BULK INSERT` command to load data from csv Files to bronze tables.

Parameters:
    None. 
	  This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXEC bronze.load_bronze;
===============================================================================
*/
-- USE DataWarehouse;

CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
    DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME, @batch_end_time DATETIME;
    BEGIN TRY
        SET @batch_start_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        
        PRINT '================================================';
        PRINT 'LOADING BRONZE LAYER';
        PRINT '================================================';

        PRINT '------------------------------------------------';
        PRINT 'LOADING CRM TABLES';
        PRINT '------------------------------------------------';

        SET @start_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        PRINT '>> Truncating Table: bronze.crm_cust_info';
        TRUNCATE TABLE bronze.crm_cust_info;

        PRINT '>> Inserting Data Into: bronze.crm_cust_info';
        BULK INSERT bronze.crm_cust_info
        FROM '/var/opt/mssql/data/cust_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );

        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
        PRINT '------------------------'

        SET @start_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        PRINT '>> Truncating Table: bronze.crm_prd_info';
        TRUNCATE TABLE bronze.crm_prd_info;

        PRINT '>> Inserting Data Into: bronze.crm_prd_info';

        BULK INSERT bronze.crm_prd_info
        FROM '/var/opt/mssql/data/prd_info.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );

        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
        PRINT '------------------------'

        SET @start_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        PRINT '>> Truncating Table: bronze.crm_sales_details';
        TRUNCATE TABLE bronze.crm_sales_details;

        PRINT '>> Inserting Data Into: bronze.crm_sales_details';

        BULK INSERT bronze.crm_sales_details
        FROM '/var/opt/mssql/data/sales_details.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );

        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
        PRINT '------------------------'

        
        PRINT '------------------------------------------------';
        PRINT 'LOADING ERP TABLES'
        PRINT '------------------------------------------------';

        SET @start_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );

        PRINT '>> Truncating Table: bronze.erp_cust_az12';
        TRUNCATE TABLE bronze.erp_cust_az12;

        PRINT '>> Inserting Data Into: bronze.erp_cust_az12';

        BULK INSERT bronze.erp_cust_az12
        FROM '/var/opt/mssql/data/CUST_AZ12.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
        PRINT '------------------------'

        SET @start_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        PRINT '>> Truncating Table: bronze.erp_loc_a101';
        TRUNCATE TABLE bronze.erp_loc_a101;

        PRINT '>> Inserting Data Into: bronze.erp_loc_a101';

        BULK INSERT bronze.erp_loc_a101
        FROM '/var/opt/mssql/data/LOC_A101.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
        PRINT '------------------------'

        SET @start_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        PRINT '>> Truncating Table: bronze.erp_px_cat_g1v2';
        TRUNCATE TABLE bronze.erp_px_cat_g1v2;

        PRINT '>> Inserting Data Into: bronze.erp_px_cat_g1v2';

        BULK INSERT bronze.erp_px_cat_g1v2
        FROM '/var/opt/mssql/data/PX_CAT_G1V2.csv'
        WITH (
            FIRSTROW = 2,
            FIELDTERMINATOR = ',',
            ROWTERMINATOR = '\n',
            TABLOCK
        );
        SET @end_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );
        
        PRINT '>> Load Duration: ' + CAST(DATEDIFF(SECOND, @start_time, @end_time) AS NVARCHAR) + ' seconds'
        PRINT '------------------------'

        SET @batch_end_time =
            CAST(
                SYSDATETIMEOFFSET()
                    AT TIME ZONE 'UTC'
                    AT TIME ZONE 'India Standard Time'
                AS DATETIME
            );

        PRINT '==========================================================================';
        PRINT 'Batch Start Time: ' + CONVERT(VARCHAR(19), @batch_start_time, 120);
        PRINT 'Batch End Time: ' + CONVERT(VARCHAR(19), @batch_start_time, 120);
        PRINT 'Loading Bronze Layer is Completed.'    
        PRINT '>> Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + ' seconds'
        PRINT '==========================================================================';
    END TRY
    BEGIN CATCH
        PRINT '================================================';
        PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
        PRINT 'ERROR MESSAGE' + ERROR_MESSAGE();
        PRINT 'ERROR MESSAGE' + CAST(ERROR_NUMBER() AS NVARCHAR);
        PRINT 'ERROR MESSAGE' + CAST(ERROR_STATE() AS NVARCHAR);
        PRINT '================================================';

    END CATCH
END
