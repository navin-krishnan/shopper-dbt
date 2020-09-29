{{
	config(materialization = 'ephemeral')
}}



with drivers_list as (
    select * from {{ ref('os_drivers_list') }}   
),


orders as (
     select * from {{ ref('os_orders') }}
),


all_orders as (
   SELECT 
     driver_id,
     name,
     email,
     metro,
     local_delivered_at,
     COUNT(order_id) as order_cnt
   FROM orders
   GROUP BY 1,2,3,4,5
),


daily_stats as(
    SELECT 
       dl.driver_id,
       dl.dt,
       ao.name,
       ao.email,
       ao.metro,
       ao.local_delivered_at,
       COALESCE(ao.order_cnt, 0) as order_cnt,
       IFF(order_cnt > 0, 1,0) as present,
    
       SUM(present) 
       OVER (PARTITION BY dl.driver_id
             ORDER BY dl.dt 
             ROWS BETWEEN 6 PRECEDING and CURRENT ROW) 
        AS past_7,
    
       SUM(present) 
          OVER (PARTITION BY dl.driver_id
                ORDER BY dl.dt 
                ROWS BETWEEN 13 PRECEDING and CURRENT ROW) 
           AS past_14,
       
       SUM(present) 
          OVER (PARTITION BY dl.driver_id
                ORDER BY dl.dt 
                ROWS BETWEEN 29 PRECEDING and CURRENT ROW) 
           AS past_30   
    
    
    FROM drivers_list dl 
    LEFT JOIN all_orders ao
      ON dl.driver_id = ao.driver_id
      AND dl.dt = ao.local_delivered_at
    QUALIFY 
      ROW_NUMBER() OVER (PARTITION BY dl.driver_id ORDER BY dl.dt DESC) = 1
  )

SELECT * FROM daily_stats





