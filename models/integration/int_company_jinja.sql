{% set models = ["stg_hubspot_companies", "stg_rds_companies"] %}

with merged_companies as(
{%for model in models %}

    select name, {{ 'company_id' if 'hubspot' in model else 'null' }} as hubspot_company_id, 
    {{ 'company_id' if 'rds' in model else 'null'}} as rds_company_id from {{ ref(model) }} {% if not loop.last %} union all {% endif %} 
{% endfor %}
), deduped as(
    select max(hubspot_company_id) as hubspot_company_id, max(rds_company_id) as rds_company_id, name
    from merged_companies group by name
)
SELECT {{ dbt_utils.surrogate_key(['deduped.name']) }} as COMPANY_PK, HUBSPOT_COMPANY_ID, RDS_COMPANY_ID, deduped.NAME, ADDRESS, CITY, POSTAL_CODE, COUNTRY
FROM deduped
LEFT JOIN {{ref('stg_rds_companies')}} rds_companies on rds_companies.company_id = deduped.rds_company_id
 
