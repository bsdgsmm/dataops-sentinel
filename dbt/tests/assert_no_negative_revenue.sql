SELECT
    order_date,
    gross_order_value,
    net_revenue
FROM {{ ref('fct_daily_revenue') }}
WHERE gross_order_value < 0 OR captured_amount < 0 OR refunded_amount < 0
