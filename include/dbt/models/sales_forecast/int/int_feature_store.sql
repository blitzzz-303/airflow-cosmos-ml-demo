{{
    config(
        materialized='table',
    )
}}
with feature_prepare as (
SELECT
    sale.store_dept_pk,
    _month,
    approx_quantiles(temperature, 2)[offset(0)] as temperature_p25,
    approx_quantiles(temperature, 2)[offset(1)] as temperature_p75,
    approx_quantiles(fuel_price, 2)[offset(0)] as fuel_price_p25,
    approx_quantiles(fuel_price, 2)[offset(1)] as fuel_price_p75,
    cast(approx_quantiles(cpi, 2)[offset(0)] as numeric) as cpi_p25,
    cast(approx_quantiles(cpi, 2)[offset(1)] as numeric) as cpi_p75,
    cast(approx_quantiles(unemployment, 2)[offset(0)] as numeric) as unemployment_p25,
    cast(approx_quantiles(unemployment, 2)[offset(1)] as numeric) as unemployment_p75,
    sum(case when feature.isHoliday then 1 else 0 end) as holidays,
    type,
    max(size) _size,
FROM {{ ref('stg_sales') }} sale
    LEFT JOIN {{ ref('stg_store') }} store USING (store_id)
    LEFT JOIN {{ ref('stg_feature') }} feature USING (store_id, week_date)
WHERE week_date < date('{{ var('test_date') }}')
group by store_dept_pk, _month, type
qualify row_number() over (partition by store_dept_pk, _month) = 1
)
select
    store_dept_pk,
    _month,
    ml.standard_scaler(temperature_p25) over () as temperature_p25,
    ml.standard_scaler(temperature_p75) over () as temperature_p75,
    ml.standard_scaler(fuel_price_p25) over () as fuel_price_p25,
    ml.standard_scaler(fuel_price_p75) over () as fuel_price_p75,
    ml.standard_scaler(cpi_p25) over () as cpi_p25,
    ml.standard_scaler(cpi_p75) over () as cpi_p75,
    ml.standard_scaler(unemployment_p25) over () as unemployment_p25,
    ml.standard_scaler(unemployment_p75) over () as unemployment_p75,
    ml.standard_scaler(holidays) over () as holidays,
    ml.quantile_bucketize(_size, 5) over () as size_bucket,
    type
from feature_prepare