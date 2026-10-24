WITH ranked AS (
    SELECT
        customer_id,
        email,
        full_name,
        country,
        created_at,
        logical_date,
        ROW_NUMBER() OVER (
            PARTITION BY customer_id
            ORDER BY logical_date DESC
        ) AS version_rank
    FROM {{ source('raw', 'customers') }}
)

SELECT
    customer_id,
    email,
    full_name,
    country,
    created_at,
    logical_date AS last_seen_date
FROM ranked
WHERE version_rank = 1
