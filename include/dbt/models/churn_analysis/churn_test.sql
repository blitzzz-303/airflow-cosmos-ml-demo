with eval_data as (
    SELECT
        *,
    FROM {{ ref('churn_stg') }}
    ORDER BY phone
    LIMIT 5000
    OFFSET 3000
)

select * from {{ dbt_ml.predict(ref('churn_model'), 'eval_data') }}
