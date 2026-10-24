WITH ranked AS (
    SELECT
        order_id,
        customer_id,
        product_id,
        quantity,
        unit_price,
        order_total,
        status,
        ordered_at,
        logical_date,
        ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY logical_date DESC
        ) AS version_rank
    FROM {{ source('raw', 'orders') }}
)

SELECT
    order_id,
    customer_id,
    product_id,
    quantity,
    unit_price,
    order_total,
    status AS order_status,
    ordered_at,
    CAST(ordered_at AT TIME ZONE 'UTC' AS DATE) AS order_date,
    logical_date AS source_logical_date
FROM ranked
WHERE version_rank = 1
