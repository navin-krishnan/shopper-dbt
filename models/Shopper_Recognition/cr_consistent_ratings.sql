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
      IFF((past_4_present = 4 and past4 = 5), TRUE, FALSE ) as "Perfect 4",
      IFF((past_8_present = 8 and past8 = 5), TRUE, FALSE ) as "Perfect 8",
      IFF((past_4_present = 4 and ROUND(past4,1) = 4.8), TRUE, FALSE ) as "Good 4",
      IFF((past_8_present = 8 and ROUND(past8,1) = 4.8), TRUE, FALSE ) as "Good 8"
   FROM weekly_stats
)

SELECT * FROM final