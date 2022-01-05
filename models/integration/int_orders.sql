{% set models = ["stg_orders"] %}

with staged_orders as (
    {%for model in models%}
        select {{'order_id'}} as ORDER_ID, {{'ORDER_DATE'}}, {{'EMPLOYEE_ID'}}, {{'CUSTOMER_ID'}},
        {{'PRODUCT_ID'}}, QUANTITY, DISCOUNT, UNIT_PRICE from {{ref(model)}} {% if not loop.last %} union all {% endif %}
    {%endfor%}
)
SELECT {{ dbt_utils.surrogate_key(['PRODUCT_ID', 'ORDER_DATE', 'CUSTOMER_ID', 'order_id']) }} as ORDER_PK, ORDER_DATE, PRODUCT_ID, EMPLOYEE_ID,
CUSTOMER_ID, QUANTITY, DISCOUNT, UNIT_PRICE FROM staged_orders