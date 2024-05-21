INSERT INTO STV202311139__DWH.dm_transactions(transaction_id, operation_id, trans_type_id, load_dt, load_src)
SELECT HASH(v.operation_id) AS transaction_id
     , v.operation_id::uuid AS operation_id
     , tt.trans_type_id
     , now() AS load_dt
     , 'pg' AS load_src
  FROM (SELECT DISTINCT t.operation_id
	     , t.transaction_type
          FROM STV202311139__STAGING.transactions t
         WHERE t.transaction_dt BETWEEN :dt1 AND :dt2
       ) v
  JOIN STV202311139__DWH.dm_trans_types tt
    ON tt.trans_type = v.transaction_type
 WHERE NOT EXISTS (SELECT 1
                     FROM STV202311139__DWH.dm_transactions tr
                    WHERE tr.operation_id = v.operation_id::uuid
                  );
