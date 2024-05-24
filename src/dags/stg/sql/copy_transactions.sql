COPY STV202311139__STAGING.transactions(operation_id ENFORCELENGTH, account_number_from, account_number_to, currency_code, country ENFORCELENGTH, status ENFORCELENGTH, transaction_type ENFORCELENGTH, amount, transaction_dt)
FROM LOCAL STDIN DELIMITER ';' REJECTED DATA AS TABLE STV202311139__STAGING.transactions_rej;
