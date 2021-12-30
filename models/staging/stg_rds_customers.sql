--- import ctes
WITH customers as (
  SELECT * FROM {{ source('RDS', 'CUSTOMERS') }} 
),  
companies AS (
  SELECT * FROM dbt_sydhssn.stg_rds_companies
),
--- logical ctes
renamed as (
    SELECT concat('rds-', customer_id) AS customer_id, 
    SPLIT_PART(contact_name, ' ', 1) AS first_name,
    SPLIT_PART(contact_name, ' ', -1) AS last_name,
        CASE  
        WHEN LEN(translate(phone, ' ,(,),.,-', '')) = 10 THEN translate(phone, ' ,(,),.,-', '') 
        ELSE NULL 
        END AS PHONE,
    company_id
    FROM CUSTOMERS
    JOIN companies ON companies.company_name = customers.company_name
),
cleaned as (
    SELECT customer_id,
    first_name,
    last_name,
    '('||LEFT(PHONE, 3)||') '||RIGHT(LEFT(PHONE,6),3)||'-'||RIGHT(PHONE,4) AS PHONE,
    company_id
    FROM renamed
)
--- final select statement
select * FROM cleaned