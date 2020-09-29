{{
    config(materialization = 'ephemeral')
}}

with rated_orders_weekly as (
     select * from {{ref('cr_rated_orders_weekly')}}
),


drivers_list as (
     select * from {{ ref('cr_drivers_list') }}
),


weekly_stats AS (
SELECT 
    dl.driver_id,
    dl.wk,
    rw.name,
    rw.email,
    rw.metro,
    orw.local_delivery_week,
    IFF(orw.order_cnt > 0, 1, 0) as present,
    COALESCE(orw.rating_sum, 0) as ratings,
    COALESCE(orw.order_cnt, 0) as order_cnt,

    SUM(present) 
    OVER (PARTITION BY dl.driver_id
         ORDER BY dl.wk 
         ROWS BETWEEN 3 PRECEDING and CURRENT ROW) 
    AS past_4_present,

    SUM(present) 
      OVER (PARTITION BY dl.driver_id
            ORDER BY dl.wk 
            ROWS BETWEEN 7 PRECEDING and CURRENT ROW) 
       AS past_8_present,


    SUM(rating_sum) 
    OVER (PARTITION BY dl.driver_id
         ORDER BY dl.wk 
         ROWS BETWEEN 3 PRECEDING and CURRENT ROW)/ SUM(order_cnt) OVER 
                                                    (PARTITION BY dl.driver_id
                                                     ORDER BY dl.wk 
                                                     ROWS BETWEEN 3 PRECEDING and CURRENT ROW) 
    as past4,

    SUM(rating_sum) 
    OVER (PARTITION BY dl.driver_id
         ORDER BY dl.wk 
         ROWS BETWEEN 7 PRECEDING and CURRENT ROW)/ SUM(order_cnt) OVER 
                                                    (PARTITION BY dl.driver_id
                                                     ORDER BY dl.wk 
                                                     ROWS BETWEEN 7 PRECEDING and CURRENT ROW) 
    as past8

FROM drivers_list dl 
LEFT JOIN rated_orders_weekly orw
  ON dl.driver_id = orw.driver_id
  AND dl.wk = orw.local_delivery_week
JOIN (SELECT DISTINCT driver_id, name, email, metro FROM rated_orders_weekly) as rw 
  ON dl.driver_id = rw.driver_id     
QUALIFY 
  ROW_NUMBER() OVER (PARTITION BY dl.driver_id ORDER BY dl.wk DESC) = 1  
)

SELECT * FROM weekly_stats