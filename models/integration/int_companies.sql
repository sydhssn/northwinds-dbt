WITH hubspot_companies as (
    SELECT * FROM {{ ref('stg_hubspot_companies') }}
), rds_companies as (
    SELECT * FROM {{ ref('stg_rds_companies') }}
), merged_companies as(
    SELECT COMPANY_ID AS HUBSPOT_COMPANY_ID, NULL AS RDS_COMPANY_ID, NAME
    FROM hubspot_companies UNION ALL
    SELECT NULL AS HUBSPOT_COMPANY_ID, COMPANY_ID AS RDS_COMPANY_ID, NAME
    FROM rds_companies
), combined_companies as(
    SELECT MAX(HUBSPOT_COMPANY_ID) AS HUBSPOT_COMPANY_ID, MAX(RDS_COMPANY_ID) AS RDS_COMPANY_ID, NAME
FROM merged_companies GROUP BY NAME
), combined_companies_details as (
    SELECT HUBSPOT_COMPANY_ID, RDS_COMPANY_ID, combined_companies.NAME, ADDRESS, CITY, POSTAL_CODE, COUNTRY
    FROM combined_companies
    LEFT JOIN rds_companies ON combined_companies.NAME = rds_companies.NAME
)
SELECT {{ dbt_utils.surrogate_key(['name']) }} as COMPANY_PK, HUBSPOT_COMPANY_ID, RDS_COMPANY_ID, NAME, ADDRESS, CITY, POSTAL_CODE, COUNTRY
FROM combined_companies_details

