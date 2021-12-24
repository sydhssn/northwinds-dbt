WITH source AS (
    SELECT * FROM "PAGILA_INC"."NORTHWINDS_RDS_PUBLIC"."SUPPLIERS"
), contact_names AS (SELECT SUPPLIER_ID, COUNTRY, 
    SPLIT_PART(contact_name, ' ', 1) as contact_first_name,
    SPLIT_PART(contact_name, ' ', -1) as contact_last_name,
    ADDRESS, CITY, 
    CASE  
        WHEN LEN(translate(phone, ' ,(,),.,-', '')) = 10 THEN translate(phone, ' ,(,),.,-', '') 
        ELSE NULL 
        END AS PHONE, 
    COMPANY_NAME, REGION, POSTAL_CODE, FAX, CONTACT_TITLE, HOMEPAGE, _FIVETRAN_DELETED, _FIVETRAN_SYNCED 
    FROM source
), cleaned_contact_names AS (
    SELECT SUPPLIER_ID, COUNTRY, contact_first_name, contact_last_name, ADDRESS, CITY,
    '('||LEFT(PHONE, 3)||')'||RIGHT(LEFT(PHONE,6),3)||'-'||RIGHT(PHONE,4) AS PHONE,
    COMPANY_NAME, REGION, POSTAL_CODE, FAX, CONTACT_TITLE, HOMEPAGE, _FIVETRAN_DELETED, _FIVETRAN_SYNCED
    FROM contact_names
)
SELECT * FROM cleaned_contact_names

