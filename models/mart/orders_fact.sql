{{ config(materialized='table') }}
WITH int_contacts as (
    select * from {{ ref('int_contacts') }}
),
int_orders as (
    select * from {{ ref('int_orders') }}
),
combined_tables as(
    select * from int_orders
    LEFT JOIN int_contacts ON int_contacts.rds_contact_id = int_orders.customer_id
)
SELECT ORDER_PK, CONTACT_PK, ORDER_DATE, PRODUCT_ID, EMPLOYEE_ID, QUANTITY, DISCOUNT, UNIT_PRICE FROM combined_tables
ORDER BY ORDER_DATE ASC