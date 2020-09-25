SELECT
        driver_id,
        local_delivery_week,
        COUNT(order_id) as total_orders

    FROM (
        SELECT 
            o.id as order_id,
            o.driver_id,
            DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, o.delivered_at::timestamp_ntz))::date 
            as local_delivery_week
        FROM OG_VIEWS.ORDERS as o 
        JOIN DATA_SCIENCE.OPS_METROS as m on o.metro_id = m.metro_id
        WHERE o.status = 'delivered'
        
        UNION ALL
        
        SELECT 
            ed.id as order_id,
            ed.driver_id,
            DATE_TRUNC('week', convert_timezone('UTC', m.time_zone, ed.delivered_at::timestamp_ntz))::date 
            as local_delivery_week
        FROM OG_VIEWS.ENVOY_DELIVERIES as ed 
        JOIN DATA_SCIENCE.OPS_METROS as m on ed.metro_id = m.metro_id
        WHERE ed.status = 'delivered'
        ) as orders_list
    GROUP BY 1,2
    ORDER BY 1,2