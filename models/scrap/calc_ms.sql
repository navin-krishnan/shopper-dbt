with order_stats as (
     
     select * from {{ ref('driver_orders_ms') }}           
) 

   SELECT 
     DISTINCT s.driver_id,
     
     (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 15
       AND driver_id = s.driver_id) as first15,
    
    (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 30
       AND driver_id = s.driver_id) as first30,
   
   (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 40
       AND driver_id = s.driver_id) as first40,
   
   (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 50
       AND driver_id = s.driver_id) as first50       
   
   FROM order_stats s
