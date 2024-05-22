import vertica_python


class VerticaClient:
    def __init__(self, params, logger):
        self.__params = params
        self.__logger = logger

    @staticmethod
    def __get_sql(sql_filepath):
        with open(sql_filepath, "r") as f:
            return f.read()

    def import_data(self, sql_filepath, data_filepath):
        try:
            with vertica_python.connect(**self.__params) as conn, conn.cursor() as cur, \
                 open(data_filepath, "rb") as f:
                    sql = self.__get_sql(sql_filepath)
                    cur.copy(sql, f)
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)
            raise err
        
    def exec_query(self, sql_filepath, params):
        try:
            with vertica_python.connect(**self.__params) as conn, conn.cursor() as cur:
                sql = self.__get_sql(sql_filepath)
                cur.execute(sql, params)
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)
            raise err