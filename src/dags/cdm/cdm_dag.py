import logging
import pendulum
import os
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task_group
from lib.utils import create_tasks

# Dashboard screenshot could be found in 
# https://github.com/ppetrov91/de-project-final/blob/main/src/img/dashboard.png

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
    def load_data(logger):
        layer_type, sql_dirpath = "cdm", os.path.join(os.path.dirname(__file__), "sql")
        query_types = ("insert_fake_row", "copy_drop_partitions", "fill", "swap_partitions")
        [
            create_tasks(layer_type, sql_dirpath, query_types, obj_name, logger)
            for obj_name in ("global_metrics",)
        ]

    logger = logging.getLogger(__name__)
    t_start, t_finish = (EmptyOperator(task_id=tn) for tn in ("start", "finish"))
    t_start >> load_data(logger) >> t_finish

dag = load_data_to_cdm_dag()