-- Modèle de staging pour les commandes
-- Fichier : models/staging/stg_orders.sql
-- Nettoyage, typage, renommage

{{
    config(
        materialized='view',
        tags=['staging', 'orders']
    )
}}

WITH source AS (
    SELECT * FROM {{ source('staging', 'stg_orders') }}
),

renamed AS (
    SELECT
        -- Clés primaires
        id AS order_id,
        user_id AS customer_id,
        
        -- Timestamps
        created_at AS order_date,
        
        -- Métriques
        total_amount AS order_amount,
        
        -- Attributs
        UPPER(status) AS order_status,
        UPPER(country) AS country_code,
        LOWER(payment_method) AS payment_method,
        
        -- Métadonnées
        _loaded_at
        
    FROM source
    WHERE total_amount > 0
)

SELECT * FROM renamed