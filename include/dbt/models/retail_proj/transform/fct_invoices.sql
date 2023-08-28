WITH fct_invoices_cte AS (
    SELECT
        InvoiceNo AS invoice_id,
        InvoiceDate AS datetime_id,
        {{ dbt_utils.generate_surrogate_key(['StockCode', 'Description', 'UnitPrice']) }} as product_id,
        {{ dbt_utils.generate_surrogate_key(['CustomerID', 'Country']) }} as customer_id,
        Quantity AS quantity,
        Quantity * UnitPrice AS revenue
    FROM {{ source('retail', 'raw_invoices') }}
    WHERE Quantity > 0
)
SELECT
    invoice_id,
    PARSE_DATETIME('%m/%d/%y %H:%M', datetime_id) as invoice_processed_at,
    product_id,
    stock_code,
    customer_id,
    quantity,
    revenue
FROM fct_invoices_cte fi
INNER JOIN {{ ref('dim_datetime') }} dt USING (datetime_id)
INNER JOIN {{ ref('dim_product') }} dp USING (product_id)
INNER JOIN {{ ref('dim_customer') }} dc USING (customer_id)