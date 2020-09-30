with weekly_stats as (
     select * from {{ ref('cr_weekly_stats') }}
),

final as(
   SELECT
      driver_id as "Driver ID",
      name as "Name",
      email as "Email",
      metro as "Metro",
      wk as "Week",
      CASE
        WHEN past_8_present = 8 and past8 = 5  THEN 'Perfect 8'  
        WHEN past_4_present = 4 and past4 = 5 THEN 'Perfect 4'
        WHEN past_8_present = 8 and ROUND(past8,1) = 4.8 THEN 'Good 8'
        WHEN past_4_present = 4 and ROUND(past4,1) = 4.8 THEN 'Good 4'
        ELSE 'Other'
      END as "Performance"
   FROM weekly_stats
)

SELECT * FROM final