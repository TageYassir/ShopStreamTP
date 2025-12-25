-- Performance des produits (analyse ABC)
-- Fichier : models/marts/mart_product_performance.sql

{{
    config(
        materialized='table',
        tags=['mart', 'product']
    )
}}

WITH fact_orders AS (
    SELECT * FROM {{ ref('fact_orders') }}
    WHERE is_completed = 1
),

dim_products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

product_metrics AS (
    SELECT
        f.product_key,
        p.product_name,
        p.product_category,
        
        -- Volume
        COUNT(DISTINCT f.order_key) AS total_orders,
        SUM(f.quantity_sold) AS total_quantity_sold,
        
        -- Revenus
        SUM(f. line_revenue) AS total_revenue,
        AVG(f.unit_price) AS avg_unit_price,
        
        -- Marge
        SUM(f. estimated_margin) AS total_margin,
        SUM(f.estimated_margin) / NULLIF(SUM(f.line_revenue), 0) AS margin_rate,
        
        -- Dates
        MIN(f.order_timestamp) AS first_sale_date,
        MAX(f.order_timestamp) AS last_sale_date
        
    FROM fact_orders f
    INNER JOIN dim_products p ON f.product_key = p.product_key
    GROUP BY 1, 2, 3
),

ranked AS (
    SELECT
        *,
        -- Classement par revenu
        ROW_NUMBER() OVER (ORDER BY total_revenue DESC) AS revenue_rank,
        
        -- Cumul du CA
        SUM(total_revenue) OVER (ORDER BY total_revenue DESC ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) AS cumulative_revenue,
        
        -- CA total
        SUM(total_revenue) OVER () AS total_revenue_all
        
    FROM product_metrics
),

final AS (
    SELECT
        *,
        -- Pourcentage du CA total
        total_revenue / total_revenue_all AS revenue_pct,
        
        -- Pourcentage cumulé
        cumulative_revenue / total_revenue_all AS cumulative_revenue_pct,
        
        -- Classification ABC
        CASE
            WHEN cumulative_revenue / total_revenue_all <= 0.8 THEN 'A'
            WHEN cumulative_revenue / total_revenue_all <= 0.95 THEN 'B'
            ELSE 'C'
        END AS abc_class,
        
        -- Performance
        CASE
            WHEN revenue_rank <= 10 THEN 'Top 10'
            WHEN revenue_rank <= 50 THEN 'Top 50'
            ELSE 'Long Tail'
        END AS performance_tier
        
    FROM ranked
)

SELECT
    product_key,
    product_name,
    product_category,
    total_orders,
    total_quantity_sold,
    total_revenue,
    avg_unit_price,
    total_margin,
    margin_rate,
    first_sale_date,
    last_sale_date,
    revenue_rank,
    revenue_pct,
    cumulative_revenue_pct,
    abc_class,
    performance_tier
FROM final
ORDER BY revenue_rank