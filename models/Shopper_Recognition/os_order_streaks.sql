with daily_stats as (
   select * from {{ ref('os_daily_stats') }}
),



final as (
   SELECT 
      driver_id as "Driver ID",
      name as "Name",
      email as "Email",
      metro as "Metro",
      dt as "Date" ,
      local_delivered_at as "Delivery Date",
      past_7,
      past_14,
      past_30,
      CASE
       WHEN past_30 = 30 THEN '30 Day Streak'
       WHEN past_14 = 14 THEN '14 Day Streak'
       WHEN past_7 = 7 THEN '7 Day Streak'
       ELSE 'None' 
      END as "Consecutive Days Active"  
   
   FROM daily_stats
   WHERE local_delivered_at IS NOT NULL
)


SELECT * FROM final

