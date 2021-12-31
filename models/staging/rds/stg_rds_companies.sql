WITH source as (
  SELECT * FROM {{ source('RDS', 'CUSTOMERS') }} 
),  
renamed AS (
    SELECT
    concat('rds-', replace(lower(company_name), ' ', '-')) AS company_id,
    company_name as name,
    max(address) AS address,
    max(city) AS city,
    max(postal_code) AS postal_code,
    max(country) AS country
    FROM source
    GROUP BY company_name
)
SELECT * FROM renamed