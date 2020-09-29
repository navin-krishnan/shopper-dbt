{{
  config(materialization = 'ephemeral')
}}



with drivers_list as (
    select * from {{ ref('ww_drivers_list') }}   
),


weekend_orders as (
     select * from {{ ref('ww_weekend_orders') }}
),


weekly_stats as(
   SELECT 
      dl.driver_id,
      dl.wk,
      rw.name,
      rw.email,
      rw.metro,
      wo.local_delivery_week,
      COALESCE(wo.order_cnt, 0) as order_cnt,
      IFF(order_cnt > 0, 1, 0) as present,
   
      SUM(present) 
      OVER (PARTITION BY dl.driver_id
            ORDER BY dl.wk 
            ROWS BETWEEN 1 PRECEDING and CURRENT ROW) 
       AS past_2,
   
      SUM(present) 
         OVER (PARTITION BY dl.driver_id
               ORDER BY dl.wk 
               ROWS BETWEEN 3 PRECEDING and CURRENT ROW) 
          AS past_4,
   
       LAG(order_cnt, 4) OVER (PARTITION BY dl.driver_id order by dl.wk) as lag5,
       LAG(order_cnt, 5) OVER (PARTITION BY dl.driver_id order by dl.wk) as lag6
      
   
   FROM drivers_list dl 
   LEFT JOIN weekend_orders wo
     ON dl.driver_id = wo.driver_id
     AND dl.wk = wo.local_delivery_week
   JOIN (SELECT DISTINCT driver_id, name, email, metro FROM weekend_orders) as rw 
     ON dl.driver_id = rw.driver_id   
   QUALIFY 
     ROW_NUMBER() OVER (PARTITION BY dl.driver_id ORDER BY dl.wk DESC) = 1
)

SELECT * FROM weekly_stats