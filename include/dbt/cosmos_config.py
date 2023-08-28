# include/dbt/cosmos_config.py

from cosmos.config import ProfileConfig, ProjectConfig
from pathlib import Path

airflow_sys_path = Path('/opt/airflow/')

DBT_CONFIG = ProfileConfig(
    profile_name='airflow_learn',
    target_name='dev',
    profiles_yml_filepath=Path(f'{airflow_sys_path}/include/dbt/profiles.yml')
)

DBT_PROJECT_CONFIG = ProjectConfig(
    dbt_project_path=f'{airflow_sys_path}/include/dbt/',
)
