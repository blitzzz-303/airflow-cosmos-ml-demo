import sys
airflow_home = '/opt/airflow/'
sys.path.insert(0, airflow_home)

# import the local file system to gcs operator
from airflow.providers.google.cloud.transfers.local_to_gcs import LocalFilesystemToGCSOperator
# import bash operator
from airflow.operators.bash import BashOperator

# import dbt task group
from cosmos.airflow.task_group import DbtTaskGroup
from cosmos.config import RenderConfig
from cosmos.constants import LoadMode
from include.dbt.cosmos_config import DBT_PROJECT_CONFIG, DBT_CONFIG

# import DAG
from airflow.decorators import dag
# import datetime
from datetime import datetime


# define the DAG
@dag(
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['sales_forecast'],
)
def sales_forecast():
    upload_to_gcs = LocalFilesystemToGCSOperator(
        task_id='upload_to_gcs',
        gcp_conn_id='gcp',
        src=[f'{airflow_home}include/dataset/ds2_feature_dataset.csv',
             f'{airflow_home}include/dataset/ds2_sales_dataset.csv',
             f'{airflow_home}include/dataset/ds2_store_dataset.csv'],
        dst='raw/',
        bucket='blitzzz-airflow-test',
        mime_type='text/csv',
    )
    
    bash_cmd = f'. {airflow_home}dbt_venv/bin/activate '
    bash_cmd += f'&& cd {airflow_home}include/dbt/ '
    bash_cmd += "&& dbt run-operation stage_external_sources --args 'select: sales_forecast' --vars '{ext_full_refresh: true}'"
    config_external_table = BashOperator(
        task_id='config_external_table',
        bash_command=bash_cmd,
    )
    
    dbt_transform = DbtTaskGroup(
        group_id='dbt_transform',
        project_config=DBT_PROJECT_CONFIG,
        profile_config=DBT_CONFIG,
        render_config=RenderConfig(
            load_method=LoadMode.DBT_LS,
            select=['path:models/sales_forecast'],
        )
    )

    upload_to_gcs >> config_external_table >> dbt_transform

sales_forecast_dag = sales_forecast()
