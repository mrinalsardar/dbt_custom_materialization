select
    store,
    to_date(date, 'DD/MM/YYYY') as feature_date,
    temperature,
    fuel_price,
    markdown1,
    markdown2,
    markdown3,
    markdown4,
    markdown5,
    cpi,
    unemployment,
    isholiday
from
    {{ ref('features') }}