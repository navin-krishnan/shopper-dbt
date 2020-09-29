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
      IFF(past_7 = 7, TRUE, FALSE) AS "7 Day Streak",
      IFF(past_14 = 14, TRUE, FALSE) AS "14 Day Streak",
      IFF(past_30 = 30, TRUE, FALSE) AS "30 Day Streak"
   
   FROM daily_stats
   WHERE local_delivered_at IS NOT NULL
)


SELECT * FROM final

