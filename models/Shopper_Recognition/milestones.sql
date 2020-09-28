WITH ods AS (
SELECT 
      o.id as order_id,
      o.driver_id as driver_id,
      d.name,
      d.email,
      m.metro,
      convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz) as local_delivered_at,
      DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date as local_delivery_week,
      o.order_lines_count,
      IFF(o.order_lines_count > 25, 1,0) as large_order,


      (CASE 
         WHEN ar.wrong_items = false AND ar.damaged_items = false 
              AND ar.v2_late_delivery = false AND ar.v2_poor_substitution_choices = false 
              AND ar.unfriendly_driver = false AND ar.missing_items = false
              AND r.ignore = false
         THEN 1 else 0 end)::int as good


FROM OG_VIEWS.ORDERS as o
JOIN OG_VIEWS.DRIVERS as d on d.id = o.driver_id
JOIN DATA_SCIENCE.OPS_METROS as m on d.metro_id = m.metro_id
LEFT JOIN OG_VIEWS.RATINGS as r on o.id = r.order_id
LEFT JOIN NG_VIEWS.AVIATOR_ORDER_RATING as ar on o.id = ar.order_id  
WHERE o.STATUS = 'delivered' 
      AND d.email not ilike '%@shipt%'
      and d.disabled_at IS NULL
      and d.deactivated_at IS NULL
      AND DATE_TRUNC('week', local_delivered_at) >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '4 WEEKS'

UNION ALL

SELECT 
      ed.id as order_id,
      ed.driver_id as driver_id,
      d.name,
      d.email,
      m.metro,
      convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz) as local_delivered_at,
      DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz))::date as local_delivery_week,
      NULL as order_lines_count,
      NULL as large_order,
      NULL as good
FROM OG_VIEWS.ENVOY_DELIVERIES as ed
JOIN OG_VIEWS.DRIVERS as d on d.id = ed.driver_id
JOIN DATA_SCIENCE.OPS_METROS as m on d.metro_id = m.metro_id
WHERE ed.STATUS = 'delivered' 
      AND d.email not ilike '%@shipt%'
      and d.disabled_at IS NULL
      and d.deactivated_at IS NULL
      AND DATE_TRUNC('week', local_delivered_at) >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '4 WEEKS'
),

order_stats AS(
SELECT
    driver_id,
    local_delivery_week,
    COUNT(order_id) as total_orders
FROM ods
GROUP BY 1,2  
),


milestones AS (
   SELECT 
     DISTINCT s.driver_id,

     (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 1
       AND driver_id = s.driver_id) as first,

     
     (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 15
       AND driver_id = s.driver_id) as first15,
    
    (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 30
       AND driver_id = s.driver_id) as first30,
   
   (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 40
       AND driver_id = s.driver_id) as first40,
   
   (SELECT
       MIN(local_delivery_week) FROM order_stats
       WHERE total_orders >= 50
       AND driver_id = s.driver_id) as first50       
   
   FROM order_stats s
),



aggregated as (

     SELECT 
        driver_id,
        DATE_TRUNC('week', CURRENT_DATE) as week,
        CASE 
          WHEN first50 = DATE_TRUNC('week', CURRENT_DATE)
            THEN 'First 50'
          WHEN first40 = DATE_TRUNC('week', CURRENT_DATE)
            THEN 'First 40'
          WHEN first30 = DATE_TRUNC('week', CURRENT_DATE)
            THEN 'First 30'
          WHEN first15 = DATE_TRUNC('week', CURRENT_DATE)
            THEN 'First 15'
          WHEN first = DATE_TRUNC('week', CURRENT_DATE)
            THEN 'First Order'
          ELSE 'None'
        END as milestone,
        IFF(first = DATE_TRUNC('week', CURRENT_DATE), TRUE, FALSE) as first_order_ever
     FROM milestones

UNION ALL
     
     SELECT 
        driver_id,
        DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK') as week,
        CASE 
          WHEN first50 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK')
            THEN 'First 50'
          WHEN first40 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK')
            THEN 'First 40'
          WHEN first30 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK')
            THEN 'First 30'
          WHEN first15 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK')
            THEN 'First 15'
          WHEN first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK')
            THEN 'First Order'
          ELSE 'None'
        END as milestone,
        IFF(first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '1 WEEK'), TRUE, FALSE) as first_order_ever
     FROM milestones
     
UNION ALL

     SELECT 
        driver_id,
        DATE_TRUNC('week', CURRENT_DATE - INTERVAL '2 WEEKS') as week,
        CASE 
          WHEN first50 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '2 WEEKS')
            THEN 'First 50'
          WHEN first40 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '2 WEEKS')
            THEN 'First 40'
          WHEN first30 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '2 WEEKS')
            THEN 'First 30'
          WHEN first15 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '2 WEEKS')
            THEN 'First 15'
          WHEN first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '2 WEEKS')
            THEN 'First Order' 
          ELSE 'None'
        END as milestone,
        IFF(first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '2 WEEKS'), TRUE, FALSE) as first_order_ever
     FROM milestones

UNION ALL

     SELECT 
        driver_id,
        DATE_TRUNC('week', CURRENT_DATE - INTERVAL '3 WEEKS') as week,
        CASE 
          WHEN first50 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '3 WEEKS')
            THEN 'First 50'
          WHEN first40 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '3 WEEKS')
            THEN 'First 40'
          WHEN first30 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '3 WEEKS')
            THEN 'First 30'
          WHEN first15 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '3 WEEKS')
            THEN 'First 15'
          WHEN first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '3 WEEKS')
            THEN 'First Order'   
          ELSE 'None'
        END as milestone,
        IFF(first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '3 WEEKS'), TRUE, FALSE) as first_order_ever
     FROM milestones
     
UNION ALL

     SELECT 
        driver_id,
        DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 WEEKS') as week,
        CASE 
          WHEN first50 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 WEEKS')
            THEN 'First 50'
          WHEN first40 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 WEEKS')
            THEN 'First 40'
          WHEN first30 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 WEEKS')
            THEN 'First 30'
          WHEN first15 = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 WEEKS')
            THEN 'First 15'
          WHEN first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 WEEKS')
            THEN 'First Order'   
          ELSE 'None'
        END as milestone,
        IFF(first = DATE_TRUNC('week', CURRENT_DATE - INTERVAL '4 WEEKS'), TRUE, FALSE) as first_order_ever
     FROM milestones

),

first_rating AS(
    SELECT 
          order_id,
          driver_id,
          name,
          email,
          metro,
          local_delivered_at,
          local_delivery_week,
          good
    FROM ods
          
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY driver_id, local_delivery_week ORDER BY local_delivered_at) = 1
),

large_orders AS(
    SELECT 
      driver_id,
      local_delivery_week,
      SUM(large_order) as large_orders,
      CASE
        WHEN SUM(large_order) >= 16 THEN 1
        ELSE 0
      END as plus_16,
      CASE
        WHEN SUM(large_order) >= 12  and SUM(large_order) < 16 THEN 1
        ELSE 0
      END as plus_12
    FROM ods
    GROUP BY 1,2
)



SELECT 
   ag.driver_id as "Driver ID",
   fr.name,
   fr.email,
   fr.metro,
   ag.week as "Week",   
   ag.milestone as "Milestone",
   fr.order_id as "Week First Order",
   fr.local_delivery_week as "WFO Week",
   fr.local_delivered_at::date as "WFO Delivered At",
   fr.good as "Good Order",
   lo.local_delivery_week as "LO Week",
   COALESCE(lo.large_orders, 0) as "Large Orders"
   --lo.plus_12 as "Over 12",
   --lo.plus_16 as "Over 16"
FROM aggregated ag
LEFT JOIN first_rating fr 
ON ag.driver_id = fr.driver_id
   AND ag.week = fr.local_delivery_week
LEFT JOIN large_orders lo 
ON ag.driver_id = lo.driver_id
   AND ag.week = lo.local_delivery_week
WHERE fr.order_id IS NOT NULL
ORDER BY 2 DESC;

