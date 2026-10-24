SELECT
    products.product_id,
    products.product_name,
    products.category,
    products.current_unit_price,
    COUNT(orders.order_id) AS orders_count,
    COALESCE(SUM(orders.quantity) FILTER (WHERE orders.is_revenue_order), 0) AS units_sold,
    COALESCE(SUM(orders.net_paid_amount), 0) AS net_revenue,
    ROUND(
        COUNT(orders.order_id) FILTER (WHERE NOT orders.is_revenue_order)::NUMERIC
        / NULLIF(COUNT(orders.order_id), 0),
        4
    ) AS cancellation_rate,
    MAX(orders.order_date) AS last_order_date
FROM {{ ref('stg_products') }} AS products
LEFT JOIN {{ ref('int_orders_enriched') }} AS orders
    ON products.product_id = orders.product_id
GROUP BY
    products.product_id,
    products.product_name,
    products.category,
    products.current_unit_price
