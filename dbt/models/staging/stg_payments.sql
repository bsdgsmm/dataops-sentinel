WITH ranked AS (
    SELECT
        payment_id,
        order_id,
        amount,
        method,
        status,
        paid_at,
        logical_date,
        ROW_NUMBER() OVER (
            PARTITION BY payment_id
            ORDER BY logical_date DESC
        ) AS version_rank
    FROM {{ source('raw', 'payments') }}
)

SELECT
    payment_id,
    order_id,
    amount,
    method AS payment_method,
    status AS payment_status,
    paid_at,
    logical_date AS source_logical_date
FROM ranked
WHERE version_rank = 1
