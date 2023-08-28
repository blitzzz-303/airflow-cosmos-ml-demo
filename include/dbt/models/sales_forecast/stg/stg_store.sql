WITH
    store_cte AS (
        SELECT * except (store),
        store store_id
        FROM {{ source('sales_forecast', 'ds2_store_dataset') }}
    )

SELECT * FROM store_cte
