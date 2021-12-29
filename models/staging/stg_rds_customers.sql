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
    company_id
    FROM CUSTOMERS
    JOIN companies ON companies.company_name = customers.company_name
)
--- final select statement
select * FROM renamed