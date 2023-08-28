WITH
    feature_cte AS (
        SELECT
            * EXCEPT (date, store),
            store store_id,
            date week_date
        FROM {{ source('sales_forecast', 'ds2_feature_dataset') }}
    )

SELECT * FROM feature_cte
