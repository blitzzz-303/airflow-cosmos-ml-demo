{{
    config(
        materialized='view',
    )
}}
{%- set feature_engineering_flds %}
    extract(year from invoice_processed_at) invoice_processed_at_year,
    extract(month from invoice_processed_at) invoice_processed_at_month,
    extract(day from invoice_processed_at) invoice_processed_at_day,
    extract(dayofweek from invoice_processed_at) invoice_processed_at_dayofweek,
    extract(dayofyear from invoice_processed_at) invoice_processed_at_dayofyear,
{% endset -%}

WITH prep_data AS (
    SELECT
        DATE(invoice_processed_at) invoice_processed_at,
        stock_code,
        SUM(revenue) revenue
    FROM {{ ref('fct_invoices') }}
    WHERE stock_code IN ('22906', '16161U', '22706', '22584', '22089', '21213', '21212', '22582')
    GROUP BY 1, 2
)
SELECT
    *,
    {{ feature_engineering_flds }}
FROM prep_data