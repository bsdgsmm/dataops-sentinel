SELECT
    orders.order_id,
    orders.customer_id,
    orders.product_id,
    products.category AS product_category,
    customers.country AS customer_country,
    orders.quantity,
    orders.unit_price,
    orders.order_total,
    orders.order_status,
    orders.ordered_at,
    orders.order_date,
    orders.order_status <> 'cancelled' AS is_revenue_order,
    COALESCE(payments.captured_amount, 0) AS captured_amount,
    COALESCE(payments.refunded_amount, 0) AS refunded_amount,
    COALESCE(payments.net_paid_amount, 0) AS net_paid_amount,
    COALESCE(payments.failed_attempts, 0) AS failed_payment_attempts
FROM {{ ref('stg_orders') }} AS orders
LEFT JOIN {{ ref('stg_products') }} AS products
    ON orders.product_id = products.product_id
LEFT JOIN {{ ref('stg_customers') }} AS customers
    ON orders.customer_id = customers.customer_id
LEFT JOIN {{ ref('int_order_payments') }} AS payments
    ON orders.order_id = payments.order_id
