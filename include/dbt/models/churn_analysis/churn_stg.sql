{{
    config(
        materialized='view',
        dataset='churn_analysis',
    )
}}
select
    state,
    day_mins,
    day_calls,
    eve_mins,
    eve_calls,
    night_mins,
    night_calls,
    intl_mins,
    intl_calls,
    custserv_calls,
    phone,
    if(is_churn = 'True.', 'yes', 'no') is_churn
from {{ source('churn_analysis', 'churn')}}

