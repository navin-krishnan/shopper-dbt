{{
  config(materialization = 'ephemeral')
}}

with ORDERS as (
  select * from {{ ref('src_og_views_orders') }} 
),

ENVOY_DELIVERIES as (
  select * from {{ ref('src_og_views_envoy_deliveries') }} 
),

OPS_METROS as (
   select * from {{ ref('src_data_science_ops_metros') }} 
),

RATINGS as (
   select * from {{ ref('src_og_views_ratings') }}
),

DRIVERS as (
   select * from {{ ref('src_og_views_drivers') }}
),


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
     FROM ORDERS as o
     JOIN DRIVERS as d on d.id = o.driver_id
     JOIN OPS_METROS as m on d.metro_id = m.metro_id
     JOIN RATINGS as r on o.id = r.order_id
     WHERE d.email not ilike '%@shipt%'
       AND d.disabled_at IS NULL
       AND d.deactivated_at IS NULL
       AND (r.ignore = FALSE OR r.ignore IS NULL)
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
)

SELECT * FROM rated_orders_weekly    