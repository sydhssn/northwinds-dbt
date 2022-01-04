{% set models = ["stg_hubspot_contacts", "stg_rds_customers"] %}

with merged_contacts as (
    {%for model in models %}
        select {{'contact_id' if 'hubspot' in model else 'null'}} as HUBSPOT_CONTACT_ID, 
        {{'customer_id' if 'rds' in model else 'null'}}  as RDS_CONTACT_ID, FIRST_NAME, LAST_NAME, PHONE, 
        {{ 'company_id' if 'hubspot' in model else 'null' }} AS HUBSPOT_COMPANY_ID, 
        {{ 'company_id' if 'rds' in model else 'null' }} AS RDS_COMPANY_ID
        from {{ref(model)}} {% if not loop.last %} union all {% endif %} 
    {%endfor%}
)
select {{ dbt_utils.surrogate_key(['first_name', 'last_name', 'phone']) }} as CONTACT_PK, HUBSPOT_CONTACT_ID, RDS_CONTACT_ID, 
FIRST_NAME, LAST_NAME, PHONE, HUBSPOT_COMPANY_ID, RDS_COMPANY_ID
from merged_contacts