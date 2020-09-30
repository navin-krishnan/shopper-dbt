{{
	config(materialization = 'ephemeral')
}}

with orders as (
     select * from {{ ref('ms_orders') }}
),


order_stats AS(
SELECT
    driver_id,
    local_delivery_week,
    COUNT(order_id) as total_orders,
    SUM(good) as good_orders
FROM orders
GROUP BY 1,2  
)

SELECT * FROM order_stats

