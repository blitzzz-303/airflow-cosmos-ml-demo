{{
    config(
        materialized='table'
    )
}}
{%- set feature_engineering_flds %}
extract(year from invoice_processed_at) as invoice_processed_at_year,
extract(month from invoice_processed_at) as invoice_processed_at_month,
extract(day from invoice_processed_at) as invoice_processed_at_day,
extract(dayofweek from invoice_processed_at) as invoice_processed_at_dayofweek,
extract(dayofyear from invoice_processed_at) as invoice_processed_at_dayofyear,
{% endset -%}

with eval_data as (
    select *,
        {{ feature_engineering_flds }}
    from unnest(['22906', '16161U', '22706', '22584', '22089', '21213', '21212', '22582']) stock_code,
    unnest(generate_date_array('2011-12-09', '2012-06-01', INTERVAL 1 week))  invoice_processed_at
)

select * from {{ dbt_ml.predict(ref('sale_model'), 'eval_data') }}
