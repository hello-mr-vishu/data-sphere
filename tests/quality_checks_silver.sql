-- Check for null or duplicates in Primary Key
-- Expectation: No Result
SELECT 
cst_id,
COUNT(*)
FROM silver.crm_cust_info
GROUP BY cst_id
HAVING COUNT(*)>1 OR cst_id IS NULL;

-- Check for unwanted spaces
-- Expectaions : No Results
SELECT cst_firstname
FROM silver.crm_cust_info
WHERE cst_firstname != TRIM(cst_firstname)

--Data Standardization and Consistency
SELECT DISTINCT cst_gndr
FROM silver.crm_cust_info

SELECT *
FROM silver.crm_cust_info



-- ************************ crm_prd_info ********************************
-- Quality Checks
-- Check for Nulls or Duplicates in Primary Key
-- Expectation : No Result

SELECT 
prd_id,
COUNT(*)
FROM silver.crm_prd_info
GROUP BY prd_id
HAVING COUNT(*)>1 OR prd_id IS NULL;

-- Check for unwanted spaces
-- Expectaions : No Results

SELECT prd_nm
FROM silver.crm_prd_info
WHERE prd_nm != TRIM(prd_nm)

-- Check for Nulls or Negative Numbers
-- Expectation : No Results
SELECT prd_cost
FROM silver.crm_prd_info
WHERE prd_cost <0 OR prd_cost IS NULL

--Data Standardization and Consistency
SELECT DISTINCT prd_line
FROM silver.crm_prd_info

-- Check for invalid Date orders
SELECT *
FROM bronze.crm_prd_info
WHERE prd_end_dt < prd_start_dt

SELECT prd_key, prd_start_dt, prd_end_dt
FROM silver.crm_prd_info
ORDER BY prd_key, prd_start_dt;



-- ************************erp_cust_az12 ********************************
-- Quality Checks
-- Check for Nulls or Duplicates in Primary Key
-- Expectation : No Result


-- Identify out of range dates
select distinct 
bdate 
from bronze.erp_cust_az12
where bdate <'1924-01-01' or bdate > getdate()

-- Data standardization and consistency
select distinct gen ,
case when upper(trim(gen)) in ('F','Female') then 'Female'
	 when upper(trim(gen)) in ('M','Male') then 'Male'
else 'n/a'
end as gen
from bronze.erp_cust_az12

----------------------- Silver --------------------------

-- Identify out of range dates
select distinct 
bdate 
from silver.erp_cust_az12
where bdate <'1924-01-01' or bdate > getdate()

-- Data standardization and consistency
select distinct gen 
from silver.erp_cust_az12



select * from silver.erp_cust_az12
