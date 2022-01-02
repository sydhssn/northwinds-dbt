{{ config(materialized='table') }}
--- company_dim.sql
WITH int_contacts as (
    select * from {{ ref('int_contacts') }}
),
int_companies as (
    select * from {{ ref('int_companies') }}
),
combined_tables as(
    select * from int_contacts
    LEFT JOIN int_companies ON int_companies.hubspot_company_id = int_contacts.hubspot_company_id
    OR int_companies.rds_company_id = int_contacts.rds_company_id
)
SELECT CONTACT_PK, FIRST_NAME, LAST_NAME, PHONE, COMPANY_PK FROM combined_tables
