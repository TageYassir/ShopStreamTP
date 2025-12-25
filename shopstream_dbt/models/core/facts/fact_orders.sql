-- Table de faits des commandes
-- Fichier : models/core/facts/fact_orders.sql
-- Granularité : ligne de commande

{{
    config(
        materialized='table',
        tags=['core', 'fact']
    )
}}

WITH orders AS (
    SELECT * FROM {{ source('staging', 'stg_orders') }}
),

order_items AS (
    SELECT * FROM {{ source('staging', 'stg_order_items') }}
),

final AS (
    SELECT
        -- Clés de faits
        oi.id AS order_item_key,
        o.id AS order_key,
        
        -- Clés étrangères (dimensions)
        o.user_id AS customer_key,
        oi.product_id AS product_key,
        TO_DATE(o.created_at) AS date_key,
        
        -- Attributs dégénérés
        o.status AS order_status,
        o. country AS country_code,
        o.payment_method,
        
        -- Timestamps
        o.created_at AS order_timestamp,
        
        -- Métriques additives
        oi.quantity AS quantity_sold,
        oi.unit_price,
        oi.line_total AS line_revenue,
        o.total_amount AS order_total,
        
        -- Métriques dérivées
        oi.line_total * 0.2 AS estimated_margin,
        
        -- Flags
        CASE WHEN o.status IN ('paid', 'shipped', 'delivered') THEN 1 ELSE 0 END AS is_completed,
        CASE WHEN o.status = 'cancelled' THEN 1 ELSE 0 END AS is_cancelled
        
    FROM order_items oi
    INNER JOIN orders o ON oi. order_id = o.id
    WHERE o.created_at >= '{{ var("start_date") }}'
)

SELECT * FROM final