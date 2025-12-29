/*
#######################
Stored Procedure: Load Silver Layer (Bronze -> Silver)
#######################
Script Purpose:
    This stored procedure performs the ETL (Extract,Transform,Load) process to populate the 'silver' schema tables from the 'bronze' schema.
    Action Performed:
    -Truncate Silver tables.
    -Inserts transformed and cleansed data from Bronze into Silver Tables.

Parameters:
    None.
    This stored procedure does not accept any parameters or return any values.

Usage Example:
    EXECUTE silver.load_silver;
########################
*/

CREATE OR ALTER PROCEDURE silver.load_silver AS
BEGIN
	DECLARE @start_time DATETIME,@end_time DATETIME
	DECLARE @start_time_silver DATETIME,@end_time_silver DATETIME
	BEGIN TRY
		SET @start_time_silver=GETDATE();
		PRINT '################################################';
		PRINT 'Loading Silver Layer';
		PRINT '################################################';

		PRINT '------------------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '--------------------------------------------'

		SET @start_time=GETDATE();
		-- ####### Truncate & Insertion In Silver Layer for crm_cust_info Table
		PRINT '>> Truncating Table : silver.crm_cust_info';
		TRUNCATE TABLE silver.crm_cust_info;
		PRINT '>> Inserting Data Into : silver.crm_cust_info';
		INSERT INTO silver.crm_cust_info(cst_id,cst_key,cst_firstname,cst_lastname,cst_material_status,cst_gndr,cst_create_date)
		select cst_id,
		cst_key,
		TRIM(cst_firstname) AS cst_firstname,
		TRIM(cst_lastname) AS cst_lastname,
		CASE WHEN UPPER(TRIM(cst_material_status))='S' THEN 'Single'
			 WHEN UPPER(TRIM(cst_material_status))='M' THEN 'Married'
			 ELSE 'Unknown' -- Normalize marterial status values to readable format
		END cst_material_status,
		CASE WHEN UPPER(TRIM(cst_gndr))='F' THEN 'Female'
			 WHEN UPPER(TRIM(cst_gndr))='M' THEN 'Male'
			 ELSE 'Unknown'
		END cst_gndr, --Normalize gender values to readable format
		cst_create_date
		from(
		select *,ROW_NUMBER() OVER(PARTITION BY cst_id ORDER BY cst_create_date DESC) as flag_last from bronze.crm_cust_info WHERE
		cst_id IS NOT NULL) t 
		WHERE flag_last =1; --Select the most recent record per customer
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		SET @start_time=GETDATE();
		-- ####### Truncate & Insertion In Silver Layer for prd_info Table
		PRINT '>> Truncating Table : silver.crm_prd_info';
		TRUNCATE TABLE silver.crm_prd_info;
		PRINT '>> Inserting Data Into : silver.crm_prd_info';
		INSERT INTO silver.crm_prd_info(prd_id,cat_id,prd_key,prd_nm,prd_cost,prd_line,prd_start_dt,prd_end_dt)
		select
		prd_id,
		REPLACE(SUBSTRING(prd_key,1,5), '-','_') AS cat_id, --Extract category ID
		SUBSTRING(prd_key,7,LEN(prd_key)) AS prd_key,	    --Extract product key
		prd_nm,
		ISNULL(prd_cost,0) AS prd_cost,
		CASE UPPER(TRIM(prd_line))
			 WHEN 'M' THEN 'Mountain'
			 WHEN 'R' THEN 'Road'
			 WHEN 'S' THEN 'Other Sales'
			 WHEN 'T' THEN 'Touring'
			 ELSE 'Unknown' END as prd_line,				 --Map product line codes to descriptive values
		CAST(prd_start_dt as DATE) as prd_start_dt,
		CAST(LEAD(prd_start_dt) OVER(PARTITION BY prd_key ORDER BY prd_start_dt)-1 as DATE) AS prd_end_dt --Calculate end date as one day before the next start date
		FROM bronze.crm_prd_info;
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		SET @start_time=GETDATE();
		--###### Truncate & Insertion in Silver Layer for crm_sales_details
		PRINT '>> Truncating Table : silver.crm_sales_details';
		TRUNCATE TABLE silver.crm_sales_details;
		PRINT '>> Inserting Data Into : silver.crm_sales_details';
		INSERT INTO silver.crm_sales_details(
		sls_ord_num,sls_prd_key,sls_cust_id,sls_order_dt,sls_ship_dt,sls_due_dt,sls_sales,sls_quantity,sls_price)
		select 
		sls_ord_num,
		sls_prd_key,
		sls_cust_id,
		CASE WHEN sls_order_dt=0 OR LEN(sls_order_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_order_dt AS VARCHAR) AS DATE)
			 END AS sls_order_dt,
		CASE WHEN sls_ship_dt=0 OR LEN(sls_ship_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_ship_dt AS VARCHAR) AS DATE)
			 END AS sls_ship_dt,
		CASE WHEN sls_due_dt=0 OR LEN(sls_due_dt)!=8 THEN NULL
			 ELSE CAST(CAST(sls_due_dt AS VARCHAR) AS DATE)
			 END AS sls_due_dt,
		CASE WHEN sls_sales IS NULL OR sls_sales<=0 OR sls_sales!=sls_quantity*ABS(sls_price)
			 THEN sls_quantity*ABS(sls_price)
			 ELSE sls_sales END as sls_sales,		--Recalculate sales if original value is missing or incorrect
		sls_quantity,
		CASE WHEN sls_price IS NULL OR sls_price<=0
			THEN sls_sales / NULLIF(sls_quantity,0)
			ELSE sls_price END AS sls_price			--Derive price if original value is invalid
		from bronze.crm_sales_details;
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'
		PRINT '------------------------------------------------';
		PRINT 'Loading ERP Tables';
		PRINT '------------------------------------------------';

		SET @start_time=GETDATE();
		-- ####### Truncate & Insertion in Silver Layer for erp_cust_az12
		PRINT '>> Truncating Table : silver.erp_cust_az12';
		TRUNCATE TABLE silver.erp_cust_az12;
		PRINT '>> Inserting Data Into : silver.erp_cust_az12';
		INSERT INTO silver.erp_cust_az12(cid,bdate,gen)
		select 
		CASE WHEN cid LIKE 'NAS%' THEN SUBSTRING(cid,4,LEN(cid))
			ELSE cid END as cid,
		CASE WHEN bdate>GETDATE() THEN NULL
			 ELSE bdate --Set future birthdates to Null
		END AS bdate,
		CASE WHEN UPPER(TRIM(gen)) IN ('F','FEMALE') THEN 'Female'
			 WHEN UPPER(TRIM(gen)) IN ('M','MALE') THEN 'Male'
			 ELSE 'Unknown' END as gen --Normalize gender values and handle unknown case
		from bronze.erp_cust_az12;
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		SET @start_time=GETDATE();
		-- ####### Truncate & Insertion in Silver Layer for erp_loc_a101
		PRINT '>> Truncating Table : silver.erp_loc_a101';
		TRUNCATE TABLE silver.erp_loc_a101;
		PRINT '>> Inserting Data Into : silver.erp_loc_a101';
		INSERT INTO silver.erp_loc_a101(CID,CNTRY)
		select REPLACE(CID,'-','') CID,
		CASE WHEN TRIM(CNTRY)='DE' THEN 'Germany'
			 WHEN TRIM(CNTRY) IN ('US','USA') THEN 'United States'
			 WHEN TRIM(CNTRY)='' OR CNTRY IS NULL THEN 'Unknown'
			 ELSE TRIM(CNTRY) END as CNTRY  --Normalize and Handle missing or blank country codes.
		from bronze.erp_loc_a101;
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'

		SET @start_time=GETDATE();
		-- ###### Truncate & Insertion in Silver Layer for erp_px_cat_g1v2
		PRINT '>> Truncating Table : silver.erp_px_cat_g1v2';
		TRUNCATE TABLE silver.erp_px_cat_g1v2;
		PRINT '>> Inserting Data Into : silver.erp_px_cat_g1v2';
		INSERT INTO silver.erp_px_cat_g1v2(ID,CAT,SUBCAT,MAINTENANCE)
		select 
		ID,
		CAT,
		SUBCAT,
		MAINTENANCE
		from bronze.erp_px_cat_g1v2;
		SET @end_time=GETDATE();
		PRINT '>> Load Duration: ' +CAST(DATEDIFF(second,@start_time,@end_time) as NVARCHAR) + ' Seconds';
		PRINT '>>**************'
		SET @end_time_silver=GETDATE();
		PRINT '########################################'
		PRINT 'Loading Silver Layer Completed'
		PRINT '>> Total Load Duration: ' +CAST(DATEDIFF(second,@start_time_silver,@end_time_silver) as NVARCHAR) + ' Seconds';
		PRINT '#######################################'
	END TRY
	BEGIN CATCH
		PRINT '#########################################';
		PRINT 'ERROR OCCURED DURING LOADING SILVER LAYER';
		PRINT 'Error Message' + ERROR_MESSAGE();
		PRINT 'Error Message' + CAST(ERROR_NUMBER() AS VARCHAR);
		PRINT 'Error Message' + CAST(ERROR_STATE() AS VARCHAR);
		PRINT '#########################################';
	END CATCH
END

EXECUTE silver.load_silver
