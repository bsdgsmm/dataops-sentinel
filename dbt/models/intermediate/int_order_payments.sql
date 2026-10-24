SELECT
    order_id,
    COUNT(*) AS payment_events,
    COUNT(*) FILTER (WHERE payment_status = 'failed') AS failed_attempts,
    COALESCE(SUM(amount) FILTER (WHERE payment_status = 'captured'), 0) AS captured_amount,
    COALESCE(SUM(amount) FILTER (WHERE payment_status = 'refunded'), 0) AS refunded_amount,
    COALESCE(SUM(amount) FILTER (WHERE payment_status = 'captured'), 0)
    - COALESCE(SUM(amount) FILTER (WHERE payment_status = 'refunded'), 0) AS net_paid_amount,
    MIN(paid_at) FILTER (WHERE payment_status = 'captured') AS first_captured_at
FROM {{ ref('stg_payments') }}
GROUP BY order_id
