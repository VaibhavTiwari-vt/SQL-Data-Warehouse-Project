/*
#########################
Quality Checks
#########################
Script Purpose:
    This script performs various quality checks for data consistency, accuracy, and standardization across the 'silver' schemas.
    It includes checks for:
    -Null or Duplicate primary keys.
    -Unwanted space in string fields.
    -Data standardization and consistency.
    -Invalid data ranges and orders.
    -Data consistency between related fields.

Usage Notes:
    -Run these checks after data loading Silver Layer.
    -Investigate and resolve any discrepancies found during the checks.
########################
*/

--Check for duplicates and null in primary key.
select cst_id,count(*) from bronze.crm_cust_info group by cst_id having count(*)>1 OR cst_id IS NULL;

--Check for unwanted spaces in string value.
select cst_firstname from bronze.crm_cust_info
where cst_firstname!=TRIM(cst_firstname);
select cst_lastname from bronze.crm_cust_info
where cst_lastname!=TRIM(cst_lastname);
select cst_gndr from bronze.crm_cust_info
where cst_gndr!=TRIM(cst_gndr);

--Data Standardization & Consistency
SELECT DISTINCT cst_gndr FROM bronze.crm_cust_info
SELECT DISTINCT cst_material_status FROM bronze.crm_cust_info
select * from bronze.crm_cust_info where cst_material_status is NULL;

--Check for Invalid Dates

SELECT NULLIF(sls_order_dt,0)
FROM bronze.crm_sales_details
WHERE sls_order_dt<=0 OR LEN(sls_order_dt)!=8;

SELECT NULLIF(sls_ship_dt,0)
FROM bronze.crm_sales_details
WHERE sls_ship_dt<=0 OR LEN(sls_ship_dt)!=8;

SELECT NULLIF(sls_due_dt,0)
FROM bronze.crm_sales_details
WHERE sls_due_dt<=0 OR LEN(sls_due_dt)!=8;

SELECT * from silver.crm_sales_details
where sls_order_dt > sls_ship_dt OR sls_order_dt>sls_due_dt;

--Check For Data Consistency: Between Sales, Quanity, and Price
-- >> Sales=Quantity * Price
-- >> Values must not be NULL, zero , or Negative

SELECT distinct
sls_sales,
sls_quantity,
sls_price
FROM silver.crm_sales_details
WHERE sls_sales!= sls_quantity*sls_price 
OR sls_sales IS NULL OR sls_quantity IS NULL OR sls_price IS NULL
OR sls_sales <=0 OR sls_quantity <=0 OR sls_price <=0
order by sls_sales,sls_quantity,sls_price;

SELECT * FROM silver.crm_sales_details;

--Identify Out of Range Dates
SELECT DISTINCT bdate from silver.erp_cust_az12 WHERE bdate>GETDATE();

--Data Standarization & Consistency
SELECT DISTINCT gen FROM silver.erp_cust_az12;

--CID Checking
SELECT cid FROM silver.erp_loc_a101;

--Standardization & Consistency Check
SELECT DISTINCT CNTRY FROM silver.erp_loc_a101 ORDER BY CNTRY;

--Check for unwanted Spaces
SELECT * FROM bronze.erp_px_cat_g1v2
WHERE CAT!=TRIM(CAT) OR SUBCAT!=TRIM(SUBCAT) OR MAINTENANCE!=TRIM(MAINTENANCE);

--Data Standardization & Consistency
SELECT DISTINCT MAINTENANCE FROM bronze.erp_px_cat_g1v2;
