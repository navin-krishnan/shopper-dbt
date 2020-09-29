with daily_stats as (
   select * from {{ ref('os_daily_stats') }}
)



SELECT * FROM daily_stats

