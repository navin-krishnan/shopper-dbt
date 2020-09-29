{{
	config(materialization = 'ephemeral')
}}

with ORDERS as (
     select * from {{ref( 'src_og_views_orders' )}}
),

ENVOY_DELIVERIES as (
     select * from {{ref( 'src_og_views_envoy_deliveries' )}}

),


day_series as(
SELECT 
  DATEADD('day', ROW_NUMBER() OVER (ORDER BY NULL) , DATE('2020-08-03')) as dt
FROM 
  TABLE(GENERATOR(ROWCOUNT => 10000))
QUALIFY dt < CURRENT_DATE
),


drivers_list as(
   with dr as( 
        SELECT 
           DISTINCT o.driver_id
           FROM ORDERS o
           WHERE o.delivered_at::date >= DATE('2020-08-03')
        
        UNION 
        
        SELECT 
           DISTINCT ed.driver_id
           FROM ENVOY_DELIVERIES ed
           WHERE ed.delivered_at::date >= DATE('2020-08-03')
        )
    SELECT 
      dr.driver_id,
      ds.dt
    FROM dr 
    
    CROSS JOIN day_series ds
    ORDER BY 1,2
)

SELECT * FROM drivers_list

