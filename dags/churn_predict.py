from airflow.decorators import dag
from datetime import datetime
# import gcs operator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyDatasetOperator
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain

# import /opt/airflow/ to sys.path
import sys
sys.path.insert(0, '/opt/airflow/')
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.constants import LoadMode
from cosmos.config import RenderConfig
@dag(
    start_date=datetime(2021, 1, 1),
    catchup=False,
    tags=['churn_predict'],
)

def churn():
    csv_file = 'churn.csv'
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        gcp_conn_id='gcp',
        src=f'/opt/airflow/include/dataset/{csv_file}',
        dst=f'raw/{csv_file}',
        bucket='blitzzz-airflow-test',
        mime_type='text/csv',

    )

    create_dataset = BigQueryCreateEmptyDatasetOperator(
        task_id='create_dataset',
        dataset_id='churn_analysis',
        gcp_conn_id='gcp',
    )

    # form a bash script
    # activate virtualenv
    bash_cmd = '. /opt/airflow/dbt_venv/bin/activate '
    bash_cmd += '&& cd /opt/airflow/include/dbt/ '
    bash_cmd += "&& dbt run-operation stage_external_sources --args 'select: churn_analysis' --vars '{ext_full_refresh: true}'"
    gcs_to_bq = BashOperator(
        task_id='gcs_to_bq',
        bash_command=bash_cmd,
    )

    transform = DbtTaskGroup(
        group_id='churn_ml',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/churn_analysis']
        )
    )

    # dbt run-operation stage_external_sources --args "select: churn_analysis" --vars '{"ext_full_refresh": true}'

    chain(
        upload_to_gcs,
        create_dataset,
        gcs_to_bq,
        transform
    )

churn()