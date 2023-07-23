{# select
    s.Store,
    s.Date,
    sum(s.Weekly_sales) as Weekly_sales
from
    {{ ref('sales_data') }} s
group by
    s.Store, s.Date #}

select "Store" from {{ ref('sales_data') }} limit 10