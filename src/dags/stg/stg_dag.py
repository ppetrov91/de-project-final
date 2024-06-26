import logging
import pendulum
import os
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task_group
from lib.utils import create_tasks


@dag(
    dag_id = 'load_to_staging', 
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['final-project', 'stg', 'schema', 'data_load', 'project'],
    is_paused_upon_creation=True
)
def load_data_to_stg_dag():
    @task_group(group_id="load_data_from_pg_to_stg")
    def load_data_to_stg(logger):
        sql_dirpath = os.path.join(os.path.dirname(__file__), "sql")
        layer_type, query_types = "stg", ("copy", )

        [create_tasks(layer_type, sql_dirpath, query_types, obj_name, logger) 
         for obj_name in ("currencies", "transactions")]

    logger = logging.getLogger(__name__)
    t_start, t_finish = (EmptyOperator(task_id=tn) for tn in ("start", "finish"))
    t_start >> load_data_to_stg(logger) >> t_finish

dag = load_data_to_stg_dag()