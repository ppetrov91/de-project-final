INSERT INTO STV202311139__DWH.fct_trans_status(id, transaction_id, transaction_dt, transaction_status, load_dt, load_src)
SELECT HASH(tr.transaction_id, t.transaction_dt) AS id
     , tr.transaction_id
     , t.transaction_dt
     , t.transaction_status
     , now() AS load_dt
     , 'pg' AS load_src
  FROM (SELECT DISTINCT t.operation_id::uuid AS operation_id
             , t.transaction_dt
             , t.status AS transaction_status 
          FROM STV202311139__STAGING.transactions t
         WHERE t.transaction_dt BETWEEN :dt1 AND :dt2
	   AND t.account_number_from > 0
           AND t.account_number_to > 0
       ) t
  JOIN STV202311139__DWH.dm_transactions tr
    ON tr.operation_id = t.operation_id
 WHERE NOT EXISTS (SELECT 1
                     FROM STV202311139__DWH.fct_trans_status ts
                    WHERE ts.transaction_id = tr.transaction_id
                      AND ts.transaction_dt = t.transaction_dt
                  );
