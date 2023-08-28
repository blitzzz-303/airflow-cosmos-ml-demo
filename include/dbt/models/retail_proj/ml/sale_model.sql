{% if var('ml_enabled', false) %}
    {{
        config(
            materialized='model',
            ml_config={
                'model_type': 'BOOSTED_TREE_REGRESSOR',
                'input_label_cols':['revenue'],
            }
        )
    }}

    SELECT
        * EXCEPT (invoice_processed_at),
    FROM {{ ref('sale_prep') }}
{% else %}
    {{
        config(alias='sale_model_skip_training',)
    }}
    select 
        'false' as ml_enabled,
        current_timestamp() as execution_ts,
    FROM {{ ref('sale_prep') }}
{% endif %}