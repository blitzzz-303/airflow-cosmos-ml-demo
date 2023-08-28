WITH
    sale_cte AS (
        SELECT
            * except(store),
            extract(month from date) as _month,
            store store_id,
            date week_date,
            concat(store, '#', dept) store_dept_pk
        FROM {{ source('sales_forecast', 'ds2_sales_dataset') }}
    )

SELECT * FROM sale_cte
WHERE store_id in ({{ var('stores')}}) AND dept in ({{ var('depts')}})