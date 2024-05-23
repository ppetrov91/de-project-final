# Итоговый проект

### Структура проекта
1. src/dags/cdm/sql - SQL-скрипты для построения витрины данных global_metrics.
2. src/dags/cdm/cdm_dag.py - Airflow DAG для построения витрины данных global_metrics.
3. src/dags/dds/sql - SQL-скрипты для наполнения dds-слоя.
4. src/dags/dds/dds_dag.py - Airflow DAG для наполнения dds-слоя.
5. src/libs/pg_client.py - класс для работы с СУБД PostgreSQL.
6. src/libs/utils.py - код, используемый Airflow dags при загрузке данных, создании dds слоя и построения витрины.
7. src/libs/vertica_client.py - код для работы с СУБД Vertica.
8. src/stg/sql - SQL-скрипты выгрузки данных из СУБД PostgreSQL и наполнения stg-слоя.
9. src/stg/stg_dag.py - Airflow DAG для наполнения stg-слоя.
10. src/ddl - SQL-скрипты создания STG, DDS и CDM слоя.
11. src/img - screenshot dashboard.

### Описание настроечных параметров
Все настроечные параметры хранятся в виде переменных Airflow. Ниже приведены их описания:
  - LOAD_DATE - дата загрузки. Если указано 0001-01-01, то берётся текущая временная отметка. Затем из неё вычитается один день.    Она и будет использоваться в качестве параметров фильтрации при выгрузке курсов валют и транзакций из СУБД PostgreSQL.
  - PG_DBNAME - наименование БД-источника в СУБД PostgreSQL.
  - PG_HOST - адрес хоста СУБД PostgreSQL.
  - PG_OUTPUT_DIR - директория сохранения данных по курсам валют и транзакциям.
  - PG_PASSWORD - пароль для подключения к СУБД PostgreSQL.
  - PG_PORT - порт подключения к СУБД PostgreSQL.
  - PG_USER - логин подключения к СУБД PostgreSQL.
  - VERTICA_DBNAME - наименование БД в СУБД Vertica.
  - VERTICA_HOST - наименования хоста с СУБД Vertica.
  - VERTICA_PASSWORD - пароль для подключения к СУБД Vertica.
  - VERTICA_PORT - порт для подключения к СУБД Vertica.
  - VERTICA_USER - логин подключения к СУБД Vertica.

### Описание stg-dag
1. Для работы с СУБД PostgreSQL используется psycopg, не psycopg2, поскольку psycopg позволяет использовать параметры при выполнении команды COPY.
2. Данные выгружаются за прошлый день от заказанной даты. В stg-слой данные также попадают с помощью команду COPY.
3. Задания создаются динамически, что позволит с минимальными затратами добавлять новые сущности для загрузки.

### Описание dds-слоя
Реализован по схеме снежинка:
1. dm_accounts, dm_countries, dm_currencies, dm_trans_types, dm_transactions - измерения
2. fct_currency_exchange, fct_trans_amount_status - факты.

### Описание dds-dag
1. Данные переносятся за прошлый день от заказанной даты.
2. Переносятся данные во все измерения кроме dm_transactions, поскольку dm_transactions зависит от dm_trasn_types.
3. Затем данные импортируются в dm_transactions.
4. Потом заполняются таблицы факты.

### Описание cdm-dag
1. Сначала в global_metrics добавляется фейковая строка, чтобы у основной таблицы и global_metrics_copy гарантированно существовали партиции.
2. Удаляется партиция за указанную дату в global_metrics_copy.
3. Выполняется запрос наполнения global_metrics_copy за указанную дату, получаем секцию с новыми данными.
4. Меняем местами партиции global_metrics и global_metrics_copy. 

### Dashboard
1. Результат в de-project-final УЗ ppetrov91