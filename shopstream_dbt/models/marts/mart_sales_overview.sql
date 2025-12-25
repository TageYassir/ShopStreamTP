-- Vue d'ensemble des ventes
-- Fichier : models/marts/mart_sales_overview.sql
-- Agrégation par jour, pays, catégorie

{{
    config(
        materialized='table',
        tags=['mart', 'sales']
    )
}}

WITH fact_orders AS (
    SELECT * FROM {{ ref('fact_orders') }}
),

dim_products AS (
    SELECT * FROM {{ ref('dim_products') }}
),

dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

daily_sales AS (
    SELECT
        f.date_key AS sale_date,
        f.country_code,
        p.product_category,
        c.customer_segment,
        
        -- Métriques agrégées
        COUNT(DISTINCT f.order_key) AS total_orders,
        COUNT(DISTINCT f.customer_key) AS unique_customers,
        SUM(f.quantity_sold) AS total_quantity,
        SUM(f.line_revenue) AS total_revenue,
        SUM(f.estimated_margin) AS total_margin,
        AVG(f.order_total) AS avg_order_value,
        
        -- Ratios
        SUM(f.line_revenue) / NULLIF(COUNT(DISTINCT f.order_key), 0) AS revenue_per_order,
        SUM(f.estimated_margin) / NULLIF(SUM(f.line_revenue), 0) AS margin_rate
        
    FROM fact_orders f
    INNER JOIN dim_products p ON f.product_key = p.product_key
    INNER JOIN dim_customers c ON f.customer_key = c.customer_key
    WHERE f.is_completed = 1
    GROUP BY 1, 2, 3, 4
)

SELECT * FROM daily_sales