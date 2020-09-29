{{
  config(materialization = 'ephemeral')
}}


with orders as (
     select * from {{ ref('ms_orders') }}
),


first_rating AS (
    SELECT 
          order_id,
          driver_id,
          name,
          email,
          metro,
          local_delivered_at,
          local_delivery_week,
          good
    FROM orders
          
    QUALIFY 
        ROW_NUMBER() OVER (PARTITION BY driver_id, local_delivery_week 
                           ORDER BY local_delivered_at) = 1
)


SELECT * FROM first_rating