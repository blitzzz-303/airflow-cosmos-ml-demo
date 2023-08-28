{{
    config(
        materialized='table',
        partition_by={'field': 'week_date', 'data_type': 'date'},
        cluster_by='store_dept_pk'
    )
}}
with future as (
    select
        *,
        extract(month from week_date) as _month
    from 
    unnest(generate_date_array('2012-10-26', '2013-12-31', interval 7 day)) week_date,
    (select distinct store_dept_pk from airflow_sales_forecast.stg_sales)
),
eval_data as (
SELECT
    week_date as week_date,
    EXTRACT(YEAR FROM week_date) AS year,
    EXTRACT(MONTH FROM week_date) AS month,
    EXTRACT(WEEK FROM week_date) AS week,
    s.weekly_sales,
    store_dept_pk,
    f.* EXCEPT (store_dept_pk),
    case
        when week_date < date('{{ var('test_date') }}') then 'train'
        when week_date >= date('{{ var('test_date') }}') and s.weekly_sales is not null then 'test'
        else 'future'
    end as stage,
FROM {{ref('stg_sales')}} s
FULL JOIN future using (store_dept_pk, week_date, _month)
LEFT JOIN {{ ref('int_feature_store') }} f using (store_dept_pk, _month)
)

select * 
from {{ dbt_ml.predict(ref('ml_sales_forecast_model'), 'eval_data') }}
