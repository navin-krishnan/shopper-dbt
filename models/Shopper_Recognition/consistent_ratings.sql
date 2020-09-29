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
    )
    SELECT 
      dr.driver_id,
      ds.wk
    FROM dr 
    
    CROSS JOIN day_series ds
    ORDER BY 1,2
),

--Aggregate orders and ratings for the past 2 months by week
rated_orders_weekly AS (
   WITH ods as ( 
     SELECT
       o.ID as order_id,
       o.driver_id,
       d.name,
       d.email,
       m.metro,
       DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date as local_delivery_week,
       r.rating
     FROM OG_VIEWS.ORDERS as o
     JOIN OG_VIEWS.DRIVERS as d on d.id = o.driver_id
     JOIN DATA_SCIENCE.OPS_METROS as m on d.metro_id = m.metro_id
     JOIN OG_VIEWS.RATINGS as r on o.id = r.order_id
     WHERE d.email not ilike '%@shipt%'
       AND d.disabled_at IS NULL
       AND d.deactivated_at IS NULL
       AND o.STATUS = 'delivered' 
       AND local_delivery_week >= DATE_TRUNC('week', CURRENT_DATE - INTERVAL '9 WEEKS')
   )  

    SELECT 
       driver_id,
       name,
       email,
       metro, 
       local_delivery_week,
       COUNT(order_id) as order_cnt,
       SUM(rating) as rating_sum
    FROM ods
    GROUP BY 1,2,3,4,5    
),    

--Calculate avg rating for past 4 and 8 weeks
weekly_stats AS (
SELECT 
    dl.driver_id,
    dl.wk,
    rw.name,
    rw.email,
    rw.metro,
    orw.local_delivery_week,
    IFF(orw.order_cnt > 0, 1, 0) as present,
    COALESCE(orw.rating_sum, 0) as ratings,
    COALESCE(orw.order_cnt, 0) as order_cnt,

    SUM(present) 
    OVER (PARTITION BY dl.driver_id
         ORDER BY dl.wk 
         ROWS BETWEEN 3 PRECEDING and CURRENT ROW) 
    AS past_4_present,

    SUM(present) 
      OVER (PARTITION BY dl.driver_id
            ORDER BY dl.wk 
            ROWS BETWEEN 7 PRECEDING and CURRENT ROW) 
       AS past_8_present,


    SUM(rating_sum) 
    OVER (PARTITION BY dl.driver_id
         ORDER BY dl.wk 
         ROWS BETWEEN 3 PRECEDING and CURRENT ROW)/ SUM(order_cnt) OVER 
                                                    (PARTITION BY dl.driver_id
                                                     ORDER BY dl.wk 
                                                     ROWS BETWEEN 3 PRECEDING and CURRENT ROW) 
    as past4,

    SUM(rating_sum) 
    OVER (PARTITION BY dl.driver_id
         ORDER BY dl.wk 
         ROWS BETWEEN 7 PRECEDING and CURRENT ROW)/ SUM(order_cnt) OVER 
                                                    (PARTITION BY dl.driver_id
                                                     ORDER BY dl.wk 
                                                     ROWS BETWEEN 7 PRECEDING and CURRENT ROW) 
    as past8

FROM drivers_list dl 
LEFT JOIN rated_orders_weekly orw
  ON dl.driver_id = orw.driver_id
  AND dl.wk = orw.local_delivery_week
JOIN (SELECT DISTINCT driver_id, name, email, metro FROM rated_orders_weekly) as rw 
  ON dl.driver_id = rw.driver_id     
QUALIFY 
  ROW_NUMBER() OVER (PARTITION BY dl.driver_id ORDER BY dl.wk DESC) = 1  
) 


--Generate booleans if driver has consistent rating and at had least 1 order for corresponding weeks 
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
