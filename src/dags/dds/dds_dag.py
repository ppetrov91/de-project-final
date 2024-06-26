import logging
import pendulum
import os
from airflow.operators.empty import EmptyOperator
from airflow.decorators import dag, task_group
from lib.utils import create_tasks


@dag(
    dag_id = 'load_to_dds', 
    schedule_interval='@daily',
    start_date=pendulum.datetime(2022, 5, 5, tz="UTC"),
    catchup=False,
    tags=['final-project', 'dds', 'schema', 'data_load', 'project'],
    is_paused_upon_creation=True
)
def load_data_to_dds_dag():
    @task_group(group_id="load_data_from_stg_to_dds")
    def load_data(logger):
        sql_dirpath = os.path.join(os.path.dirname(__file__), "sql")
        layer_type, query_types = "dds", ("fill",)

        dm_before_trans = [
            create_tasks(layer_type, sql_dirpath, query_types, obj_name, logger)
            for obj_name in ("dm_accounts", "dm_countries", "dm_currencies", "dm_trans_types")
        ]

        dm_trans = create_tasks(layer_type, sql_dirpath, query_types, "dm_transactions", logger)
        
        fill_facts = [
            create_tasks(layer_type, sql_dirpath, query_types, obj_name, logger)
            for obj_name in ("fct_currency_exchange", "fct_trans_amount_status")
        ]

        dm_before_trans >> dm_trans >> fill_facts

    logger  = logging.getLogger(__name__)
    t_start, t_finish = (EmptyOperator(task_id=tn) for tn in ("start", "finish"))
    t_start >> load_data(logger) >> t_finish

dag = load_data_to_dds_dag()