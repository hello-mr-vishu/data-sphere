/*
========================================================================================================
Stored Procedure: Load Silver Layer (Bronze -> Silver)
========================================================================================================
Script Purpose:
  This stored procedure perfoms the ETL (Extract, Transform, Load) process to
  populate the 'silver' schema tables from the 'bronze' schema.
Actions Performed:
  - Truncates Silver tables
  - Inserts transformed and cleansed data from Bronze into Silver tables.

Parameters:
  None.
  This stored procedure does not accept ay parameters or return any values.

Usage Examples:
  EXEC Silver.load_silver;
========================================================================================================
*/

create or alter procedure silver.load_silver as
begin
	DECLARE @start_time DATETIME, @end_time DATETIME, @batch_start_time DATETIME , @batch_end_time 
	DATETIME;
	BEGIN TRY
		SET @batch_start_time = GETDATE();
		PRINT '====================================';
		PRINT 'Loading Silver Layer';
		PRINT '====================================';

		PRINT '------------------------------------';
		PRINT 'Loading CRM Tables';
		PRINT '------------------------------------';

		SET @start_time = GETDATE();
	print'>>Truncating Table: silver.crm_cust_info'
	truncate table silver.crm_cust_info
	print'>> Inserting data into : silver.crm_cust_info'
	INSERT INTO silver.crm_cust_info(
	cst_id,
	cst_key,
	cst_firstname,
	cst_lastname,
	cst_marital_status,
	cst_gndr,
	cst_create_date)

	SELECT
	cst_id,
	cst_key,
	TRIM(cst_firstname) AS cst_firstname,
	TRIM(cst_lastname) as cst_lastname,
	CASE WHEN UPPER(TRIM(cst_marital_status)) = 'S' THEN 'Single'
		WHEN UPPER(TRIM(cst_marital_status)) = 'M' THEN 'Married'
		ELSE 'n/a'
	END cst_marital_status,
	CASE WHEN UPPER(TRIM(cst_gndr)) = 'F' THEN 'Female'
		WHEN UPPER(TRIM(cst_gndr)) = 'M' THEN 'Male'
		ELSE 'n/a'
	END cst_gndr,
	cst_create_date
	FROM(
	SELECT 
	*,
	ROW_NUMBER() OVER (PARTITION BY cst_id  ORDER BY cst_create_date DESC) AS flag_last
	FROM bronze.crm_cust_info
	WHERE cst_id IS NOT NULL
	)t WHERE flag_last = 1	
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second , @start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-----------';

		-- Loading silver.crm_prd_info
		SET @start_time = GETDATE();
	print'>>Truncating Table: silver.crm_prd_info'
	truncate table silver.crm_prd_info
	print'>> Inserting data into : silver.crm_prd_info'
	INSERT INTO silver.crm_prd_info (
		prd_id,
		cat_id,
		prd_key,
		prd_nm,
		prd_cost,
		prd_line,
		prd_start_dt,
		prd_end_dt
	)
	SELECT 
		prd_id,
		REPLACE(SUBSTRING(prd_key, 1, 5), '-', '_') AS cat_id,
		SUBSTRING(prd_key, 7, LEN(prd_key)) AS prd_key,
		prd_nm,
		ISNULL(prd_cost, 0) AS prd_cost,
		CASE 
			WHEN UPPER(TRIM(prd_line)) = 'M' THEN 'Mountain'
			WHEN UPPER(TRIM(prd_line)) = 'R' THEN 'Road'
			WHEN UPPER(TRIM(prd_line)) = 'S' THEN 'Other Sales'
			WHEN UPPER(TRIM(prd_line)) = 'T' THEN 'Touring'
			ELSE 'n/a'
		END AS prd_line,
		CAST(prd_start_dt AS DATE) AS prd_start_dt,
		CAST(
			LEAD(prd_start_dt) OVER (PARTITION BY prd_key ORDER BY prd_start_dt) - 1 
			AS DATE
		) AS prd_end_dt
	FROM DataWarehouse.bronze.crm_prd_info;

		SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second , @start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-----------';



		-- Loading silver.crm_sales_details
		SET @start_time = GETDATE();

	print'>>Truncating Table: silver.crm_cust_sales_details'
	truncate table silver.crm_sales_details
	print'>> Inserting data into : silver.crm_sales_details'
	insert into silver.crm_sales_details (
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	sls_order_dt,
	sls_ship_dt,
	sls_due_dt,
	sls_sales,
	sls_quantity,
	sls_price
	)
	select 
	sls_ord_num,
	sls_prd_key,
	sls_cust_id,
	case when sls_order_dt = 0 or len(sls_order_dt) != 8 then null
		else cast(cast(sls_order_dt as varchar) AS date)
	end as sls_order_dt,
	case when sls_ship_dt = 0 or len(sls_ship_dt) != 8 then null
		else cast(cast(sls_ship_dt as varchar) AS date)
	end as sls_ship_dt,
	case when sls_due_dt = 0 or len(sls_due_dt) != 8 then null
		else cast(cast(sls_due_dt as varchar) AS date)
	end as sls_due_dt,

	case when sls_sales is null or sls_sales <=0 or sls_sales != sls_quantity * abs(sls_price)
		then sls_quantity * abs(sls_price)
		else sls_sales
	end as sls_sales,
	sls_quantity,
	case when sls_price is null or sls_price <=0
	then sls_price / nullif(sls_quantity,0)
	else sls_price 
	end as sls_price

	from bronze.crm_sales_details

	SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second , @start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-----------';


		PRINT '========================================================'
		PRINT 'Loading ERP Tables';
		PRINT '========================================================'

		-- Loading erp_cust_az12
		SET @start_time = GETDATE();

	print'>>Truncating Table: silver.erp_cust_az12'
	truncate table silver.erp_cust_az12
	print'>> Inserting data into : silver.erp_cust_az12'
	Insert into silver.erp_cust_az12(
	cid,
	bdate,
	gen
	)
	select 
	case when cid like 'NAS%' then SUBSTRING(cid,4,LEN(cid))
	else cid
	end as cid,
	case when bdate > GETDATE() then null
	else bdate
	end as bdate,
	case when upper(trim(gen)) in ('F','Female') then 'Female'
		 when upper(trim(gen)) in ('M','Male') then 'Male'
	else 'n/a'
	end as gen
	from bronze.erp_cust_az12
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second , @start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-----------';


		-- Loading erp_loc_a101
		SET @start_time = GETDATE();


	print'>>Truncating Table: silver.erp_loc_a101'
	truncate table silver.erp_loc_a101
	print'>> Inserting data into : silver.erp_loc_a101'
	insert into silver.erp_loc_a101
	(cid,cntry)
	select 
	REPLACE(cid,'-','') cid,
	case when trim(cntry) = 'DE' then 'GERMANY'
		when trim(cntry) IN ('US','USA') then 'UNITED STATES'
		WHEN trim(cntry) ='' OR cntry is null then 'n/a'
		else trim(cntry)
	end as cntry
	from bronze.erp_loc_a101
	SET @end_time = GETDATE();
		PRINT '>> Load Duration: '+ CAST(DATEDIFF(second , @start_time,@end_time) AS NVARCHAR)+ ' seconds';
		PRINT '-----------';

		-- Loading erp_px_cat_g1v2
		SET @start_time = GETDATE();
	print'>>Truncating Table: silver.erp_px_cat_g1v2'
	truncate table silver.erp_px_cat_g1v2
	print'>> Inserting data into : silver.erp_px_cat_g1v2'
	insert into silver.erp_px_cat_g1v2(id,cat,subcat,maintainence)
	select
	id,
	cat,
	subcat,
	maintainence
	from bronze.erp_px_cat_g1v2
	SET @end_time = GETDATE();
		PRINT '>> Load Duration; ' + CAST(DATEDIFF(second, @start_time, @end_time) AS NVARCHAR) + 'seconds';
		PRINT '>>----------';

		SET @batch_end_time = GETDATE();
		PRINT '========================================================'
		PRINT 'Loading Bronze Layer is Completed';
		PRINT '    - Total Load Duration: ' + CAST(DATEDIFF(SECOND, @batch_start_time, @batch_end_time) AS NVARCHAR) + 'seconds';
		PRINT '========================================================'

	END TRY
	BEGIN CATCH
		PRINT '========================================================'
		PRINT 'ERROR OCCURED DURING BRONZE LAYER'
		
		PRINT 'Error Message: ' + ERROR_MESSAGE();
		PRINT 'Error Number: ' + CAST(ERROR_NUMBER() AS NVARCHAR);
		PRINT 'Error State: ' + CAST(ERROR_STATE() AS NVARCHAR);

		PRINT '========================================================'
	END CATCH
end
