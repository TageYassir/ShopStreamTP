-- Customer Lifetime Value et segmentation RFM
-- Fichier : models/marts/mart_customer_ltv.sql

{{
    config(
        materialized='table',
        tags=['mart', 'customer']
    )
}}

WITH dim_customers AS (
    SELECT * FROM {{ ref('dim_customers') }}
),

fact_orders AS (
    SELECT * FROM {{ ref('fact_orders') }}
    WHERE is_completed = 1
),

customer_metrics AS (
    SELECT
        f.customer_key,
        
        -- Récence (Recency)
        MAX(f.order_timestamp) AS last_order_date,
        DATEDIFF(day, MAX(f.order_timestamp), CURRENT_DATE()) AS days_since_last_order,
        
        -- Fréquence (Frequency)
        COUNT(DISTINCT f.order_key) AS total_orders,
        DATEDIFF(day, MIN(f.order_timestamp), MAX(f.order_timestamp)) AS customer_lifespan_days,
        
        -- Montant (Monetary)
        SUM(f.line_revenue) AS lifetime_value,
        AVG(f.order_total) AS avg_order_value,
        
        -- Dates
        MIN(f.order_timestamp) AS first_order_date
        
    FROM fact_orders f
    GROUP BY 1
),

rfm_scores AS (
    SELECT
        *,
        -- Score RFM (1-5, 5 étant le meilleur)
        NTILE(5) OVER (ORDER BY days_since_last_order ASC) AS recency_score,
        NTILE(5) OVER (ORDER BY total_orders DESC) AS frequency_score,
        NTILE(5) OVER (ORDER BY lifetime_value DESC) AS monetary_score
        
    FROM customer_metrics
),

rfm_segments AS (
    SELECT
        *,
        recency_score + frequency_score + monetary_score AS rfm_total_score,
        
        -- Segmentation RFM
        CASE
            WHEN recency_score >= 4 AND frequency_score >= 4 AND monetary_score >= 4 THEN 'Champions'
            WHEN recency_score >= 3 AND frequency_score >= 3 THEN 'Loyal Customers'
            WHEN recency_score >= 4 AND frequency_score <= 2 THEN 'Promising'
            WHEN recency_score >= 3 AND monetary_score >= 3 THEN 'Potential Loyalists'
            WHEN recency_score <= 2 AND frequency_score >= 3 THEN 'At Risk'
            WHEN recency_score <= 2 AND monetary_score >= 4 THEN 'Cant Lose Them'
            WHEN recency_score <= 2 THEN 'Hibernating'
            ELSE 'Others'
        END AS rfm_segment
        
    FROM rfm_scores
),

final AS (
    SELECT
        c.customer_key,
        c.email,
        c.full_name,
        c.country_code,
        c.customer_segment,
        c.customer_status,
        
        -- Métriques RFM
        r.last_order_date,
        r.days_since_last_order,
        r.total_orders,
        r.customer_lifespan_days,
        r.lifetime_value,
        r.avg_order_value,
        r.first_order_date,
        
        -- Scores et segments
        r.recency_score,
        r.frequency_score,
        r.monetary_score,
        r.rfm_total_score,
        r.rfm_segment,
        
        -- Risque de churn
        CASE
            WHEN r.days_since_last_order > 180 THEN 'High'
            WHEN r.days_since_last_order > 90 THEN 'Medium'
            ELSE 'Low'
        END AS churn_risk
        
    FROM dim_customers c
    INNER JOIN rfm_segments r ON c.customer_key = r. customer_key
)

SELECT * FROM final