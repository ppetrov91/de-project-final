COPY STV202311139__STAGING.currencies(date_update, currency_code, currency_code_with, currency_with_div) 
FROM LOCAL STDIN DELIMITER ';' REJECTED DATA AS TABLE STV202311139__STAGING.currencies_rej;
