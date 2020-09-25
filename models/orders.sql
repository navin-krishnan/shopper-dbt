select
 driver_id,
 count(id) as order_cnt
from og_views.orders
group by 1 
limit 50