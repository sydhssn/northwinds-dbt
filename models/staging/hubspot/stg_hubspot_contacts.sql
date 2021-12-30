WITH source as (
  SELECT * FROM {{ source('RDS', 'CONTACTS') }} 
), adjusted as (SELECT
    concat('hubspot-', HUBSPOT_ID) AS CONTACT_ID,
    FIRST_NAME,
    LAST_NAME,
    translate(PHONE, ' ,(,),.,-', '') as PHONE,
    BUSINESS_NAME
    FROM source
), cleaned as (SELECT
    CONTACT_ID,
    FIRST_NAME,
    LAST_NAME,
    '('||LEFT(PHONE, 3)||') '||RIGHT(LEFT(PHONE,6),3)||'-'||RIGHT(PHONE,4) AS PHONE,
    BUSINESS_NAME
    FROM adjusted
)
SELECT cleaned.CONTACT_ID, cleaned.FIRST_NAME, cleaned.LAST_NAME, cleaned.PHONE, "DBT_SYDHSSN"."STG_HUBSPOT_COMPANIES".COMPANY_ID FROM cleaned
JOIN "DBT_SYDHSSN"."STG_HUBSPOT_COMPANIES" ON cleaned.BUSINESS_NAME = "DBT_SYDHSSN"."STG_HUBSPOT_COMPANIES".BUSINESS_NAME