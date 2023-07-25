{{
    config(
        materialized='incremental'
    )
}}

select
    *
from
    {{ ref('combined_data') }}

{% if is_incremental() %}
where
    sales_date between
        (select max(sales_date) from {{ this }})
        and (select max(sales_date) from {{ this }}) + interval '6 days'
{% endif %}

{# select * from {{ ref('combined_data') }} where sales_date =
(select min(sales_date) from {{ ref('combined_data') }}) #}