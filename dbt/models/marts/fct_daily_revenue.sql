SELECT
    order_date,
    COUNT(*) AS orders_count,
    COUNT(*) FILTER (WHERE NOT is_revenue_order) AS cancelled_orders,
    COALESCE(SUM(order_total) FILTER (WHERE is_revenue_order), 0) AS gross_order_value,
    SUM(captured_amount) AS captured_amount,
    SUM(refunded_amount) AS refunded_amount,
    SUM(net_paid_amount) AS net_revenue,
    ROUND(
        SUM(net_paid_amount) / NULLIF(COUNT(*) FILTER (WHERE is_revenue_order), 0), 2
    ) AS average_order_value,
    COUNT(DISTINCT customer_id) AS active_customers
FROM {{ ref('int_orders_enriched') }}
GROUP BY order_date
