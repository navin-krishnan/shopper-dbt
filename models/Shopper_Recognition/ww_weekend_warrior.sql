with weekly_stats as (
   select * from {{ ref('ww_weekly_stats') }}
),


final as (
   SELECT 
      driver_id as "Driver ID",
      name as "Name",
      email as "Email",
      metro as "Metro",
      wk as "Week",
      local_delivery_week,
      order_cnt as "Last Weekend Orders",
      past_2,
      past_4,
      COALESCE(lag5, 0) as lag_5,
      COALESCE(lag6, 0) as lag_6,
      CASE
        WHEN lag_5 != 0 AND lag_6 != 0 
             AND past_4 = 0
         THEN TRUE 
        ELSE FALSE 
       END as "Weekend Warrior"  
   
   FROM weekly_stats
)


SELECT * FROM final