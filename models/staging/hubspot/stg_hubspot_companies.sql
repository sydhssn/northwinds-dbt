WITH source as (
  SELECT * FROM {{ source('RDS', 'CONTACTS') }} 
), adjusted as (SELECT
    concat('hubspot-', translate(LOWER(BUSINESS_NAME), ' ', '-')) AS COMPANY_ID,
    BUSINESS_NAME as name
    FROM source
    GROUP BY BUSINESS_NAME
)
SELECT * FROM adjusted