/*
################################
DDL Script: Create Gold Views
################################
Script Purpose:
  This script creates views for the Gold layer in the data warehouse.
  The Gold layer represents the final dimensions and fact tables (Star Schema)

  Each view performs transformations and combines data from the Silver layer to produce a clean, enriched, and business-ready dataset.

Usage:
  - These views can be queried directly for analytics and reporting.
################################
*/

CREATE VIEW gold.dim_customers AS
SELECT
	ROW_NUMBER() OVER(ORDER BY cst_id) AS customer_key,
	ci.cst_id as customer_id,
	ci.cst_key as customer_number,
	ci.cst_firstname as first_name,
	ci.cst_lastname as last_name,
	la.CNTRY as country,
	ci.cst_material_status as marital_status,
	CASE WHEN ci.cst_gndr != 'Unknown' THEN ci.cst_gndr   --CRM is the Master for gender Info
		 ELSE COALESCE(ca.gen,'Unknown')
		 END as gender,
	ca.bdate as birthdate,
	ci.cst_create_date as create_date
FROM silver.crm_cust_info ci
LEFT JOIN silver.erp_cust_az12 ca 
ON ci.cst_key=ca.cid
LEFT JOIN silver.erp_loc_a101 la 
ON ci.cst_key=la.CID;
--##########################################################################################################
CREATE VIEW gold.dim_products AS
SELECT
	ROW_NUMBER() OVER(ORDER BY pn.prd_start_dt,pn.prd_key) As product_key,
	pn.prd_id as product_id,
	pn.prd_key as product_number,
	pn.prd_nm as product_name,
	pn.cat_id as category_id,
	pc.CAT as category,
	pc.SUBCAT as subcategory,
	pc.MAINTENANCE,
	pn.prd_cost as cost,
	pn.prd_line as product_line,
	pn.prd_start_dt as start_date
FROM silver.crm_prd_info pn
LEFT JOIN silver.erp_px_cat_g1v2 pc
ON pn.cat_id=pc.id
WHERE pn.prd_end_dt is null; --Filter out all historical data
--##########################################################################################################
CREATE VIEW gold.fact_sales AS
select 
sd.sls_ord_num as order_number, --Dimension
pr.product_key, --Dimension
cu.customer_key, --Dimension
sd.sls_order_dt as order_date, --Date
sd.sls_ship_dt as shipping_date, --Date
sd.sls_due_dt as due_date, --Date
sd.sls_sales as sales_amount, --Measure
sd.sls_quantity as quantity,  --Measure
sd.sls_price as price  --Measure
from silver.crm_sales_details sd
LEFT JOIN gold.dim_products pr
ON sd.sls_prd_key=pr.product_number
LEFT JOIN gold.dim_customers cu
ON sd.sls_cust_id=cu.customer_id
