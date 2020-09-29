with order_stats as (
     
     select * from {{ ref('driver_orders_ms') }}           
),


milestones as (
     
     select * from {{ ref('calc_ms') }}           
),


final as (

SELECT 
   driver_id,

   IFF(first15 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_15,
   IFF(first30 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_30,
   IFF(first40 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_40,
   IFF(first50 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as milestone_50

FROM milestones
)

select * from final