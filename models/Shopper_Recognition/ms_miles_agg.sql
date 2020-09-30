{{
  config(materialization = 'ephemeral')
}}


with order_stats as (
     select * from {{ ref('ms_order_stats') }}
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

)

SELECT * FROM aggregated




