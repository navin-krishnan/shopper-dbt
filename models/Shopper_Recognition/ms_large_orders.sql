{{
  config(materialization = 'ephemeral')
}}


with orders as (
     select * from {{ ref('ms_orders') }}
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
    FROM orders
    GROUP BY 1,2
)

SELECT * FROM large_orders