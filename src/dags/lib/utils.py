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

def get_sql_params():
    dt_start = Variable.get("LOAD_DATE", "0001-01-01")
    dt_start = dt_start != "0001-01-01" and datetime.strptime(dt_start,'%Y-%m-%d') or \
               datetime.now(pytz.timezone('Europe/Moscow'))

    dt_start = dt_start - timedelta(days=1)
    dt_stop = dt_start + timedelta(days=1) - timedelta(seconds=1)
    return {"dt1": dt_start, "dt2": dt_stop}

def export_stg_data(sql_dirpath, sql_params, obj_name, logger):
    pg_client = PGClient(get_pg_params(), logger)
    sql_filepath = os.path.join(sql_dirpath, f"get_{obj_name}.sql")
    output_filepath = os.path.join(Variable.get("PG_OUTPUT_DIR", ""), f"{obj_name}.csv")
    pg_client.load_data(sql_filepath, sql_params, output_filepath)
    return output_filepath

def create_tasks(layer_type, sql_dirpath, query_types, obj_name, logger):
    @task(task_id=f"fill_{obj_name}")
    def f():
        csv_filepath, sql_params = "", get_sql_params()
        vertica_client = VerticaClient(get_vertica_params(), logger)

        if layer_type == "stg":
            csv_filepath = export_stg_data(sql_dirpath, sql_params, obj_name, logger)

        for qt in query_types:
            sql_filepath = os.path.join(sql_dirpath, f"{qt}_{obj_name}.sql")

            if qt == "copy":
                with open(csv_filepath) as f:
                    vertica_client.exec_query(sql_filepath, None, {"copy_stdin": f})
                    continue

            vertica_client.exec_query(sql_filepath, sql_params, {})

    return f()