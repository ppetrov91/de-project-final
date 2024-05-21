INSERT INTO STV202311139__DWH.fct_trans_amount(id, transaction_id, account_from, account_to, 
                                               currency_id, country_id, transaction_dt,
                                               amount, load_dt, load_src)
SELECT HASH(d.transaction_id, af.account_id, at.account_id, c.currency_id) AS id
     , d.transaction_id
     , af.account_id AS account_from
     , at.account_id AS account_to
     , c.currency_id
     , dc.country_id
     , t.transaction_dt
     , t.amount
     , now() AS load_dt
     , 'pg' AS load_src
  FROM (SELECT DISTINCT t.operation_id::uuid AS operation_id
             , t.account_number_from
             , t.account_number_to  
             , t.transaction_dt
             , t.currency_code
             , t.country
             , t.amount
          FROM STV202311139__STAGING.transactions t
         WHERE t.status = 'queued'
           AND t.account_number_from > 0
           AND t.account_number_to > 0
           AND t.transaction_dt BETWEEN :dt1 AND :dt2
       ) t
  JOIN STV202311139__DWH.dm_transactions d
    ON d.operation_id = t.operation_id::uuid
  JOIN STV202311139__DWH.dm_accounts af
    ON af.account_number = t.account_number_from
  JOIN STV202311139__DWH.dm_accounts at
    ON at.account_number = t.account_number_to
  JOIN STV202311139__DWH.dm_currencies c
    ON c.currency_code = t.currency_code
  JOIN STV202311139__DWH.dm_countries dc
    ON dc.country_name = t.country 
 WHERE NOT EXISTS (SELECT 1
                     FROM STV202311139__DWH.fct_trans_amount fta
                    WHERE fta.transaction_id = d.transaction_id
                      AND fta.account_from = af.account_id
                      AND fta.account_to = at.account_id
                      AND fta.currency_id = c.currency_id
                  );
