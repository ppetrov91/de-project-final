CREATE SCHEMA IF NOT EXISTS STV202311139__STAGING;

CREATE TABLE IF NOT EXISTS STV202311139__STAGING.currencies (
    date_update timestamp,
    currency_code int,
    currency_code_with int,
    currency_with_div numeric(5,3),
    constraint currencies_stg_pk PRIMARY KEY(date_update, currency_code, currency_with_div) 
)
ORDER BY date_update, currency_code, currency_with_div
SEGMENTED BY HASH(date_update, currency_code, currency_with_div) ALL NODES
PARTITION BY date_update::date
GROUP BY calendar_hierarchy_day(date_update::date, 3, 2);

CREATE TABLE IF NOT EXISTS STV202311139__STAGING.transactions (
    operation_id varchar(60),
    account_number_from int,
    account_number_to int,
    currency_code int,
    country varchar(30),
    status varchar(30),
    transaction_type varchar(30),
    amount int,
    transaction_dt timestamp,
    constraint transactions_stg_pk PRIMARY KEY(transaction_dt, operation_id, account_number_from, account_number_to)
)
ORDER BY transaction_dt, operation_id
SEGMENTED BY HASH(transaction_dt, operation_id) ALL NODES
PARTITION BY transaction_dt::date
GROUP BY calendar_hierarchy_day(transaction_dt::date, 3, 2);
