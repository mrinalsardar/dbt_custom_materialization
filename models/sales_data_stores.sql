select
    store,
    to_date(date, 'DD/MM/YYYY') as sales_date,
    sum(weekly_sales) as weekly_sales
from
    {{ ref('sales_data') }}
group by
    store, to_date(date, 'DD/MM/YYYY')
