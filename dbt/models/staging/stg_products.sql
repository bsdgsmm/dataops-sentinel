WITH ranked AS (
    SELECT
        product_id,
        product_name,
        category,
        unit_price,
        updated_at,
        logical_date,
        ROW_NUMBER() OVER (
            PARTITION BY product_id
            ORDER BY logical_date DESC, updated_at DESC
        ) AS version_rank
    FROM {{ source('raw', 'products') }}
)

SELECT
    product_id,
    product_name,
    category,
    unit_price AS current_unit_price,
    updated_at AS price_updated_at
FROM ranked
WHERE version_rank = 1
