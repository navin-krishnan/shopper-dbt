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

AVIATOR_RATINGS as (
   select * from {{ ref('src_ng_views_aviator_order_rating') }}
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


FROM ORDERS as o
JOIN DRIVERS as d on d.id = o.driver_id
JOIN OPS_METROS as m on d.metro_id = m.metro_id
LEFT JOIN RATINGS as r on o.id = r.order_id
LEFT JOIN AVIATOR_RATINGS as ar on o.id = ar.order_id  
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
FROM ENVOY_DELIVERIES as ed
JOIN DRIVERS as d on d.id = ed.driver_id
JOIN OPS_METROS as m on d.metro_id = m.metro_id
WHERE ed.STATUS = 'delivered' 
      AND d.email not ilike '%@shipt%'
      and d.disabled_at IS NULL
      and d.deactivated_at IS NULL
      AND DATE_TRUNC('week', local_delivered_at) >= DATE_TRUNC('week', CURRENT_DATE) - INTERVAL '4 WEEKS'

