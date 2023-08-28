SELECT
  EXTRACT(YEAR FROM invoice_processed_at) AS year,
  EXTRACT(MONTH FROM invoice_processed_at) AS month,
  COUNT(DISTINCT fi.invoice_id) AS num_invoices,
  SUM(fi.revenue) AS total_revenue
FROM {{ ref('fct_invoices') }} fi
GROUP BY 1, 2
ORDER BY 1, 2