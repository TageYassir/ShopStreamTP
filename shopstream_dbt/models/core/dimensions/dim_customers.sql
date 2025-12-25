-- Dimension Clients
-- Fichier : models/core/dimensions/dim_customers. sql

{{
    config(
        materialized='table',
        tags=['core', 'dimension']
    )
}}

WITH users AS (
    SELECT * FROM {{ source('staging', 'stg_users') }}
),

orders_agg AS (
    SELECT
        user_id,
        MIN(created_at) AS first_order_date,
        MAX(created_at) AS last_order_date,
        COUNT(DISTINCT id) AS total_orders,
        SUM(total_amount) AS lifetime_value
    FROM {{ source('staging', 'stg_orders') }}
    WHERE status IN ('paid', 'shipped', 'delivered')
    GROUP BY user_id
),

final AS (
    SELECT
        -- Clé primaire
        u.id AS customer_key,
        
        -- Attributs démographiques
        u.email,
        u.first_name,
        u.last_name,
        u.first_name || ' ' || u.last_name AS full_name,
        UPPER(u.country) AS country_code,
        
        -- Segmentation
        CASE
            WHEN u.plan_type IN ('premium', 'enterprise') THEN 'Premium'
            ELSE 'Freemium'
        END AS customer_segment,
        
        u.plan_type,
        u.is_active,
        
        -- Dates importantes
        u.created_at AS registration_date,
        u.last_login,
        o.first_order_date,
        o.last_order_date,
        
        -- Métriques calculées
        COALESCE(o.total_orders, 0) AS total_orders,
        COALESCE(o.lifetime_value, 0) AS lifetime_value,
        
        -- Classification RFM simplifié
        CASE
            WHEN DATEDIFF(day, o.last_order_date, CURRENT_DATE()) <= 30 THEN 'Active'
            WHEN DATEDIFF(day, o.last_order_date, CURRENT_DATE()) <= 90 THEN 'At Risk'
            WHEN o.last_order_date IS NOT NULL THEN 'Churned'
            ELSE 'No Purchase'
        END AS customer_status,
        
        -- Métadonnées
        CURRENT_TIMESTAMP() AS _dbt_updated_at
        
    FROM users u
    LEFT JOIN orders_agg o ON u.id = o.user_id
)

SELECT * FROM final