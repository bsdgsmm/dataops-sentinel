WITH mart AS (
    SELECT COALESCE(SUM(net_revenue), 0) AS total
    FROM {{ ref('fct_daily_revenue') }}
),

payments AS (
    SELECT COALESCE(SUM(payments.net_paid_amount), 0) AS total
    FROM {{ ref('int_order_payments') }} AS payments
    INNER JOIN {{ ref('stg_orders') }} AS orders
        ON payments.order_id = orders.order_id
)

SELECT
    mart.total AS mart_total,
    payments.total AS payments_total
FROM mart
CROSS JOIN payments
WHERE mart.total <> payments.total
