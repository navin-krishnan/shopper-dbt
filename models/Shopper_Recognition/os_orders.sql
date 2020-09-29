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
)


SELECT 
      o.id as order_id,
      o.driver_id as driver_id,
      d.name,
      d.email,
      m.metro,
      convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz)::date as local_delivered_at,
      DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date as local_delivery_week
     
FROM ORDERS as o
JOIN DRIVERS as d on d.id = o.driver_id
JOIN OPS_METROS as m on d.metro_id = m.metro_id
WHERE o.STATUS = 'delivered' 
      AND d.email not ilike '%@shipt%'
      and d.disabled_at IS NULL
      and d.deactivated_at IS NULL
      AND DATE_TRUNC('week', local_delivered_at) >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '5 WEEKS'

UNION ALL

SELECT 
      ed.id as order_id,
      ed.driver_id as driver_id,
      d.name,
      d.email,
      m.metro,
      convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz)::date as local_delivered_at,
      DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz))::date as local_delivery_week

FROM ENVOY_DELIVERIES as ed
JOIN DRIVERS as d on d.id = ed.driver_id
JOIN OPS_METROS as m on d.metro_id = m.metro_id
WHERE ed.STATUS = 'delivered' 
      AND d.email not ilike '%@shipt%'
      and d.disabled_at IS NULL
      and d.deactivated_at IS NULL
      AND DATE_TRUNC('week', local_delivered_at) >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '5 WEEKS'

