{{
    config(
        materialized='model',
        ml_config={
            'model_type': 'BOOSTED_TREE_CLASSIFIER',
            'input_label_cols':['is_churn'],
            'L1_REG': 0.5,
            'L2_REG': 0.5,
            'SUBSAMPLE': 0.8,
            'DATA_SPLIT_METHOD': 'NO_SPLIT',
            'ENABLE_GLOBAL_EXPLAIN': True,
            'APPROX_GLOBAL_FEATURE_CONTRIB': True,
            'MAX_ITERATIONS': 20,
            'XGBOOST_VERSION': '1.1',
            'EARLY_STOP': True,
        }
    )
}}

SELECT
    * except(phone)
FROM {{ ref('churn_stg') }}
ORDER BY phone
LIMIT 3000
