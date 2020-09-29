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

DRIVERS as (
   select * from {{ ref('src_og_views_drivers') }}
),


weekend_orders as(
 WITH x as (
     SELECT
        o.driver_id,
        d.name,
        d.email,
        m.metro,
        o.id as order_id,
        DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date as local_delivery_week
     FROM ORDERS o
     JOIN DRIVERS as d on d.id = o.driver_id
     JOIN OPS_METROS as m on d.metro_id = m.metro_id
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
     FROM ENVOY_DELIVERIES ed
     JOIN DRIVERS as d on d.id = ed.driver_id
     JOIN OPS_METROS as m on d.metro_id = m.metro_id
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
)

SELECT * FROM weekend_orders


