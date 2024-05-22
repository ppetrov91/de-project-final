import psycopg

class PGClient:
    def __init__(self, params, logger):
        self.__params = params
        self.__logger = logger

    @staticmethod
    def __get_sql(sql_filepath):
        with open(sql_filepath, "r") as f:
            return f.read()

    def load_data(self, sql_filepath, params, output_filepath):
        try:
            sql = self.__get_sql(sql_filepath)

            with psycopg.connect(**self.__params) as conn, conn.cursor() as cur, \
                 open(output_filepath, "wb") as of, cur.copy(sql, params) as copy:
                     for data in copy:
                         of.write(data)
        except Exception as err:
            self.__logger.error(err, stack_info=True, exc_info=True)
            raise err
    

