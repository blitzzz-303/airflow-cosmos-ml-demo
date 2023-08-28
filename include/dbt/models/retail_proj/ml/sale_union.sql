SELECT
    stock_code,
    invoice_processed_at,
    revenue,
    0 predicted_revenue,
FROM {{ ref('sale_prep' )}}
UNION ALL
SELECT
    stock_code,
    invoice_processed_at,
    0 revenue,
    predicted_revenue,
FROM {{ ref('sale_predict') }}