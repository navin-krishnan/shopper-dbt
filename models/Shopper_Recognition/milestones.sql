
WITH order_stats AS(
    SELECT
        driver_id,
        local_delivery_week,
        COUNT(order_id) as total_orders

    FROM (
        SELECT 
            o.id as order_id,
            o.driver_id,
            DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date 
            as local_delivery_week
        FROM OG_VIEWS.ORDERS as o 
        JOIN DATA_SCIENCE.OPS_METROS as m on o.metro_id = m.metro_id
        WHERE o.status = 'delivered'
        
        UNION ALL
        
        SELECT 
            ed.id as order_id,
            ed.driver_id,
            DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz))::date 
            as local_delivery_week
        FROM OG_VIEWS.ENVOY_DELIVERIES as ed 
        JOIN DATA_SCIENCE.OPS_METROS as m on ed.metro_id = m.metro_id
        WHERE ed.status = 'delivered'
        ) as orders_list
    GROUP BY 1,2
    ORDER BY 1,2
), 

milestones AS (
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
)

SELECT 
   driver_id,

   IFF(first15 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_15,
   IFF(first30 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_30,
   IFF(first40 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_40,
   IFF(first50 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_50

FROM milestones


