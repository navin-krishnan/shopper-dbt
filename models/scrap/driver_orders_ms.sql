{{
    config(alias = 'shopper_weekly_orders')

}}

with orders as (
  select * from {{ ref('src_og_views_orders') }} 
  
),

envoy_deliveries as (
  select * from {{ ref('src_og_views_envoy_deliveries') }} 
  
),


ops_metros as (
   select * from {{ ref('src_data_science_ops_metros') }} 
  
),


orders_combined as (
   
        SELECT 
            o.id as order_id,
            o.driver_id,
            DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date 
            as local_delivery_week
        FROM orders as o 
        JOIN ops_metros as m on o.metro_id = m.metro_id
        WHERE o.status = 'delivered'
        
        UNION ALL
        
        SELECT 
            ed.id as order_id,
            ed.driver_id,
            DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz))::date 
            as local_delivery_week
        FROM envoy_deliveries as ed 
        JOIN ops_metros as m on ed.metro_id = m.metro_id
        WHERE ed.status = 'delivered'
 ),


aggregated as (
     SELECT
          driver_id,
          local_delivery_week,
          COUNT(order_id) as total_orders
     FROM orders_combined
     GROUP BY 1,2
     ORDER BY 1,2
) 

SELECT * FROM aggregated   