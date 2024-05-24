import os
import pytz
from airflow.decorators import task
from lib.pg_client import PGClient
from lib.vertica_client import VerticaClient
from airflow.models.variable import Variable
from datetime import datetime, timedelta


def get_pg_params():
    pg_params = {f"{param.lower()}": Variable.get(f"PG_{param}", "") 
                 for param in ("DBNAME", "HOST", "PORT", "USER", "PASSWORD")}
    
    pg_params["port"] = int(pg_params["port"])
    pg_params["sslmode"] = "require"
    return pg_params

def get_vertica_params():
    vertica_params = {f"{param.lower()}": Variable.get(f"VERTICA_{param}", "")
                      for param in ("DBNAME", "HOST", "PORT", "USER", "PASSWORD")}

    vertica_params["autocommit"] = True
    vertica_params["use_prepared_statements"] = False
    vertica_params["port"] = int(vertica_params["port"])
    vertica_params["database"] = vertica_params["dbname"]
    del vertica_params["dbname"]

    return vertica_params

def get_params():
    pg_params = get_pg_params()
    vertica_params = get_vertica_params()
    pg_output_dir = Variable.get("PG_OUTPUT_DIR", "")
    dt_start = Variable.get("LOAD_DATE", "0001-01-01")
    dt_start = dt_start != "0001-01-01" and datetime.strptime(dt_start,'%Y-%m-%d') or \
               datetime.now(pytz.timezone('Europe/Moscow'))

    dt_start = dt_start - timedelta(days=1)
    dt_stop = dt_start + timedelta(days=1) - timedelta(seconds=1)
    return pg_params, vertica_params, pg_output_dir, {"dt1": dt_start, "dt2": dt_stop}

def create_tasks(vertica_params, sql_dirpath, query_types, sql_params, obj_name, logger):
    @task(task_id=f"fill_{obj_name}")
    def f():
        vertica_client = VerticaClient(vertica_params, logger)
        
        for qt in query_types:
            sql_filepath = os.path.join(sql_dirpath, f"sql/{qt}_{obj_name}.sql")
            vertica_client.exec_query(sql_filepath, sql_params)
    
    return f()

def create_stg_task(pg_params, vertica_params, sql_dirpath, sql_params, output_dirpath, obj_name, logger):
    @task(task_id=f"load_{obj_name}_data_from_pg")
    def f():
        output_filepath = os.path.join(output_dirpath, f"{obj_name}.csv")
        
        pg_client = PGClient(pg_params, logger)
        sql_filepath = os.path.join(sql_dirpath, f"sql/get_{obj_name}.sql")
        pg_client.load_data(sql_filepath, sql_params, output_filepath)

        vertica_client = VerticaClient(vertica_params, logger)
        sql_filepath = os.path.join(sql_dirpath, f"sql/save_{obj_name}.sql")
        vertica_client.import_data(sql_filepath, output_filepath)

    return f()