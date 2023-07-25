select
    f.store,
    sd.type,
    sd.size,
    f.temperature,
    f.fuel_price,
    f.markdown1,
    f.markdown2,
    f.markdown3,
    f.markdown4,
    f.markdown5,
    f.cpi,
    f.unemployment,
    f.isholiday,
    sl.weekly_sales as sales,
    sl.sales_date
from
    {{ ref('features_clean') }} f
    join {{ ref('sales_data_stores') }} sl on f.store = sl.store and f.feature_date = sl.sales_date
    join {{ ref('stores_dim') }} sd on f.store = sd.store
