-- Dimension Produits
-- Fichier : models/core/dimensions/dim_products. sql

{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

WITH products AS (
    SELECT * FROM {{ source('staging', 'stg_products') }}
),

product_stats AS (
    SELECT
        product_id,
        SUM(quantity) AS total_quantity_sold,
        SUM(line_total) AS total_revenue,
        COUNT(DISTINCT order_id) AS total_orders
    FROM {{ source('staging', 'stg_order_items') }}
    GROUP BY product_id
),

final AS (
    SELECT
        -- Clé primaire
        p.id AS product_key,
        
        -- Attributs
        p.name AS product_name,
        p. description AS product_description,
        p. category AS product_category,
        p.merchant_id,
        
        -- Prix
        p.price AS current_price,
        
        -- Stock
        p.stock_quantity AS current_stock,
        
        -- Métriques calculées
        COALESCE(s.total_quantity_sold, 0) AS total_quantity_sold,
        COALESCE(s.total_revenue, 0) AS total_revenue,
        COALESCE(s.total_orders, 0) AS total_orders,
        
        -- Classification
        CASE
            WHEN s.total_revenue IS NULL THEN 'No Sales'
            ELSE 'Active'
        END AS product_status,
        
        -- Dates
        p.created_at AS product_created_at,
        p.updated_at AS product_updated_at,
        
        -- Métadonnées
        CURRENT_TIMESTAMP() AS _dbt_updated_at
        
    FROM products p
    LEFT JOIN product_stats s ON p.id = s.product_id
)

SELECT * FROM final