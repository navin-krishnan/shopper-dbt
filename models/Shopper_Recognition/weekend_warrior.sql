--Generate series of dates
WITH day_series AS(
    SELECT 
        DATEADD('week', ROW_NUMBER() OVER (ORDER BY NULL), '2020-07-06')::date as wk
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 10000))
    QUALIFY wk < DATE_TRUNC('week', CURRENT_DATE)     
),

--Create a cross table of all relevant dates for each driver
drivers_list AS(
   with dr AS( 
        SELECT 
           DISTINCT o.driver_id
           FROM OG_VIEWS.ORDERS o
           WHERE DATE_TRUNC('week', o.delivered_at::date) >= 
                   DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '9 WEEKS'
        
        UNION 
        
        SELECT 
           DISTINCT ed.driver_id
           FROM OG_VIEWS.ENVOY_DELIVERIES ed
           WHERE DATE_TRUNC('week', ed.delivered_at::date) >= 
                   DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '9 WEEKS'
        )
    SELECT 
      dr.driver_id,
      ds.wk
    FROM dr 
    
    CROSS JOIN day_series ds
    ORDER BY 1,2
),


--Get order counts for only weekends, by week
weekend_orders as(
 WITH x as (
     SELECT
        o.driver_id,
        d.name,
        d.email,
        m.metro,
        o.id as order_id,
        DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date as local_delivery_week
     FROM OG_VIEWS.ORDERS o
     JOIN OG_VIEWS.DRIVERS as d on d.id = o.driver_id
     JOIN DATA_SCIENCE.OPS_METROS as m on d.metro_id = m.metro_id
     WHERE o.STATUS = 'delivered' 
       AND d.email not ilike '%@shipt%'
       AND d.disabled_at IS NULL
       AND d.deactivated_at IS NULL
       AND DATE_TRUNC('week', local_delivery_week) >=
                  DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '9 WEEKS'
       AND DAYNAME(convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz)::date) IN ('Fri', 'Sat', 'Sun')     
             
     UNION ALL 
     
     SELECT
        ed.driver_id,
        d.name,
        d.email,
        m.metro,
        ed.id as order_id,
        DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz))::date as local_delivery_week
     FROM OG_VIEWS.ENVOY_DELIVERIES ed
     JOIN OG_VIEWS.DRIVERS as d on d.id = ed.driver_id
     JOIN DATA_SCIENCE.OPS_METROS as m on d.metro_id = m.metro_id
     WHERE ed.STATUS = 'delivered' 
       AND d.email not ilike '%@shipt%'
       AND d.disabled_at IS NULL
       AND d.deactivated_at IS NULL
       AND DATE_TRUNC('week', local_delivery_week) >=
                  DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '9 WEEKS'
       AND DAYNAME(convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz)::date) IN ('Fri', 'Sat', 'Sun')     
   ) 

SELECT 
  driver_id,
  name,
  email,
  metro,
  local_delivery_week,
  COUNT(order_id) as order_cnt
FROM x 
GROUP BY 1,2,3,4,5
),


--Calculate continuous weekend orders for every driver for past weeks
weekly_stats as(
SELECT 
   dl.driver_id,
   dl.wk,
   rw.name,
   rw.email,
   rw.metro,
   wo.local_delivery_week,
   COALESCE(wo.order_cnt, 0) as order_cnt,
   IFF(order_cnt > 0, 1, 0) as present,

   SUM(present) 
   OVER (PARTITION BY dl.driver_id
         ORDER BY dl.wk 
         ROWS BETWEEN 1 PRECEDING and CURRENT ROW) 
    AS past_2,

   SUM(present) 
      OVER (PARTITION BY dl.driver_id
            ORDER BY dl.wk 
            ROWS BETWEEN 3 PRECEDING and CURRENT ROW) 
       AS past_4,

    LAG(order_cnt, 4) OVER (PARTITION BY dl.driver_id order by dl.wk) as lag5,
    LAG(order_cnt, 5) OVER (PARTITION BY dl.driver_id order by dl.wk) as lag6
   

FROM drivers_list dl 
LEFT JOIN weekend_orders wo
  ON dl.driver_id = wo.driver_id
  AND dl.wk = wo.local_delivery_week
JOIN (SELECT DISTINCT driver_id, name, email, metro FROM weekend_orders) as rw 
  ON dl.driver_id = rw.driver_id   
QUALIFY 
  ROW_NUMBER() OVER (PARTITION BY dl.driver_id ORDER BY dl.wk DESC) = 1
)


--Generate boolean for weekend warrior if they haven't shopped for past 4 weekends but
--shopped for 2 weekends prior to that
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


