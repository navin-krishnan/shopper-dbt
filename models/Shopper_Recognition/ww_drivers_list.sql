{{
  config(materialization = 'ephemeral')
}}

with ORDERS as (
     select * from {{ref( 'src_og_views_orders' )}}
),

ENVOY_DELIVERIES as (
     select * from {{ref( 'src_og_views_envoy_deliveries' )}}

),

day_series AS(
    SELECT 
        DATEADD('week', ROW_NUMBER() OVER (ORDER BY NULL), '2020-07-06')::date as wk
    FROM 
        TABLE(GENERATOR(ROWCOUNT => 10000))
    QUALIFY wk < DATE_TRUNC('week', CURRENT_DATE)     
),

drivers_list AS(
   with dr AS( 
        SELECT 
           DISTINCT o.driver_id
           FROM ORDERS o
           WHERE DATE_TRUNC('week', o.delivered_at::date) >= 
                   DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '9 WEEKS'
        
        UNION 
        
        SELECT 
           DISTINCT ed.driver_id
           FROM ENVOY_DELIVERIES ed
           WHERE DATE_TRUNC('week', ed.delivered_at::date) >= 
                   DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '9 WEEKS'
        )
    SELECT 
      dr.driver_id,
      ds.wk
    FROM dr 
    
    CROSS JOIN day_series ds
    ORDER BY 1,2
)

SELECT * FROM drivers_list