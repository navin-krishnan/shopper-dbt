with orders as (
     select * from {{ ref('ms_orders') }}
),

order_stats as (
     select * from {{ ref('ms_order_stats') }}
),

miles_agg as (
     select * from {{ ref('ms_miles_agg') }}
),

first_order as (
     select * from {{ ref('ms_first_order') }}
),

large_orders as (
     select * from {{ ref('ms_large_orders') }}
),


final as (
   SELECT 
   ag.driver_id as "Driver ID",
   od.name as "Name",
   od.email as "Email",
   od.metro as "Metro",
   ag.week as "Week",   
   ag.milestone as "Milestone",
   fr.order_id as "Week First Order",
   fr.local_delivery_week as "WFO Week",
   fr.local_delivered_at::date as "WFO Delivered At",
   fr.good as "Good Order",
   lo.local_delivery_week as "LO Week",
   COALESCE(os.total_orders, 0) as "Total Orders",
   COALESCE(os.good_orders, 0) as "Good Orders",
   COALESCE(lo.large_orders, 0) as "Large Orders"
FROM miles_agg ag
RIGHT JOIN (SELECT DISTINCT driver_id, name, email, metro FROM orders) as od
ON ag.driver_id = od.driver_id
LEFT JOIN first_order fr 
ON ag.driver_id = fr.driver_id
   AND ag.week = fr.local_delivery_week
LEFT JOIN large_orders lo 
ON ag.driver_id = lo.driver_id
   AND ag.week = lo.local_delivery_week
LEFT JOIN order_stats os
ON ag.driver_id = os.driver_id  
   AND ag.week = os.local_delivery_week   

--WHERE fr.order_id IS NOT NULL
ORDER BY 2 DESC
)

SELECT * FROM final