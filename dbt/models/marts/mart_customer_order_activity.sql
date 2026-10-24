WITH reference_date AS (
    SELECT MAX(order_date) AS as_of_date
    FROM {{ ref('int_orders_enriched') }}
),

activity AS (
    SELECT
        customer_id,
        MIN(order_date) AS first_order_date,
        MAX(order_date) AS last_order_date,
        COUNT(*) AS orders_count,
        COUNT(*) FILTER (WHERE is_revenue_order) AS revenue_orders_count,
        SUM(net_paid_amount) AS lifetime_net_revenue
    FROM {{ ref('int_orders_enriched') }}
    GROUP BY customer_id
)

SELECT
    customers.customer_id,
    customers.country,
    customers.created_at AS customer_created_at,
    activity.first_order_date,
    activity.last_order_date,
    COALESCE(activity.orders_count, 0) AS orders_count,
    COALESCE(activity.revenue_orders_count, 0) AS revenue_orders_count,
    COALESCE(activity.lifetime_net_revenue, 0) AS lifetime_net_revenue,
    ROUND(
        activity.lifetime_net_revenue / NULLIF(activity.revenue_orders_count, 0), 2
    ) AS average_order_value,
    reference_date.as_of_date - activity.last_order_date AS days_since_last_order
FROM {{ ref('stg_customers') }} AS customers
CROSS JOIN reference_date
LEFT JOIN activity
    ON customers.customer_id = activity.customer_id
