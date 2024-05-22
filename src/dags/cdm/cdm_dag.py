import logging
import pendulum
import os
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task_group
from lib.utils import create_cdm_task, get_params


@dag(
    dag_id = 'load_to_cdm', 
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['final-project', 'cdm', 'schema', 'data_load', 'project'],
    is_paused_upon_creation=True
)
def load_data_to_cdm_dag():
    @task_group(group_id="load_data_from_dds_to_cdm")
    def load_data(vertica_params, sql_params, logger):
        [
            create_cdm_task(vertica_params, dirname, sql_params, obj_name, logger)
            for obj_name in ("global_metrics",)
        ]

    _, vertica_params, _, sql_params = get_params()
    logger, dirname = logging.getLogger(__name__), os.path.dirname(__file__)
    t_start, t_finish = (EmptyOperator(task_id=tn) for tn in ("start", "finish"))
    t_start >> load_data(vertica_params, sql_params, logger) >> t_finish

dag = load_data_to_cdm_dag()