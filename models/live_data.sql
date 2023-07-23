select
    f.*
from
    {{ ref('features') }} f
    join {{ ref('sales_data_stores') }} sl on f.store = sl.store and f.date = sl.date
