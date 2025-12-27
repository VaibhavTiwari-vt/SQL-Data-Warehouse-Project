CREATE OR ALTER PROCEDURE bronze.load_bronze AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME
	DECLARE @start_time_bronze DATETIME,@end_time_bronze DATETIME
	BEGIN TRY
		SET @start_time_bronze=GETDATE();
		PRINT '################################################';
		PRINT 'Loading Bronze Layer';
		PRINT '################################################';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>>Truncatin Table:bronze.crm_cust_info';
		TRUNCATE  TABLE bronze.crm_cust_info;
		PRINT '>>Inserting Data Into :bronze.crm_cust_info';
		BULK INSERT bronze.crm_cust_info
		FROM 'C:\Users\VAIBHAV\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\cust_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		SET @start_time=GETDATE();
		PRINT '>>Truncatin Table:bronze.crm_prd_info';
		TRUNCATE  TABLE bronze.crm_prd_info;
		PRINT '>>Inserting Data Into :bronze.crm_prd_info';
		BULK INSERT bronze.crm_prd_info
		FROM 'C:\Users\VAIBHAV\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\prd_info.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		SET @start_time=GETDATE();
		PRINT '>>Truncatin Table:bronze.crm_sales_details';
		TRUNCATE  TABLE bronze.crm_sales_details;
		PRINT '>>Inserting Data Into :bronze.crm_sales_details';
		BULK INSERT bronze.crm_sales_details
		FROM 'C:\Users\VAIBHAV\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_crm\sales_details.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time=GETDATE();
		PRINT '>>Truncatin Table:bronze.erp_cust_az12';
		TRUNCATE  TABLE bronze.erp_cust_az12;
		PRINT '>>Inserting Data Into :bronze.erp_cust_az12';
		BULK INSERT bronze.erp_cust_az12
		FROM 'C:\Users\VAIBHAV\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\CUST_AZ12.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		SET @start_time=GETDATE();
		PRINT '>>Truncatin Table:bronze.erp_loc_a101';
		TRUNCATE  TABLE bronze.erp_loc_a101;
		PRINT '>>Inserting Data Into :bronze.erp_loc_a101';
		BULK INSERT bronze.erp_loc_a101
		FROM 'C:\Users\VAIBHAV\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\LOC_A101.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		/*
    =================================================
    Stored Procedure: Load Bronze Layer (Source->Bronze)
    =================================================
    Purpose :
      This stored procedure loads data into the 'bronze' schema from external CSV files.
      It performs the following actions:
      1. Truncate the bronze table before loading data.
      2. Uses the 'BULK INSERT' command to load data from csv files to bronze tables.

    Parameter :
      None. (It does not accept any stored parameters or return any values.)

    Usage Example:
        EXECUTE bronze.load_bronze;
    ==================================================
      
    */
    SET @start_time=GETDATE();
		PRINT '>>Truncatin Table:bronze.erp_px_cat_g1v2';
		TRUNCATE  TABLE bronze.erp_px_cat_g1v2;
		PRINT '>>Inserting Data Into :bronze.erp_px_cat_g1v2';
		BULK INSERT bronze.erp_px_cat_g1v2
		FROM 'C:\Users\VAIBHAV\OneDrive\Desktop\sql-data-warehouse-project\datasets\source_erp\PX_CAT_G1V2.csv'
		WITH (
			FIRSTROW=2,
			FIELDTERMINATOR=',',
			TABLOCK
		);
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'
		SET @end_time_bronze=GETDATE();
		PRINT '########################################'
		PRINT 'Loading Bronze Layer Completed'
		PRINT '>> Total Load Duration: ' +CAST(DATEDIFF(second,@start_time_bronze,@end_time_bronze) as NVARCHAR) + ' Seconds';
		PRINT '#######################################'
	END TRY
	BEGIN CATCH
		PRINT '#########################################';
		PRINT 'ERROR OCCURED DURING LOADING BRONZE LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '#########################################';
	END CATCH
END
