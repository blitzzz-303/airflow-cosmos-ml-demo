{{
    config(
        materialized='model',
        ml_config={
            'model_type': 'BOOSTED_TREE_REGRESSOR',
            'BOOSTER_TYPE': 'GBTREE',
            'INPUT_LABEL_COLS': ['weekly_sales'],
            'MAX_ITERATIONS': 30,
            'L1_REG': 0.5,
            'L2_REG': 0.5,
            'SUBSAMPLE': 0.8,
            'DATA_SPLIT_METHOD': 'NO_SPLIT',
            'ENABLE_GLOBAL_EXPLAIN': True,
            'APPROX_GLOBAL_FEATURE_CONTRIB': True,
            'XGBOOST_VERSION': '1.1',
            'EARLY_STOP': True,
        }
    )
}}

SELECT
    -- feature engineering split week_date into year, month, day
    EXTRACT(YEAR FROM week_date) AS year,
    EXTRACT(MONTH FROM week_date) AS month,
    EXTRACT(WEEK FROM week_date) AS week,
    s.weekly_sales,
    s.store_dept_pk,
    f.* EXCEPT (store_dept_pk)
FROM {{ref('stg_sales')}} s
LEFT JOIN {{ ref('int_feature_store') }} f using (store_dept_pk, _month)
WHERE week_date < date('{{ var('test_date') }}')
