INSERT INTO STV202311139__DWH.dm_trans_types(trans_type_id, trans_type, load_dt, load_src)
SELECT HASH(v.trans_type) AS trans_type_id
     , v.trans_type
     , now() AS load_dt
     , 'pg' AS load_src
  FROM (SELECT DISTINCT t.transaction_type AS trans_type
          FROM STV202311139__STAGING.transactions t
         WHERE t.transaction_dt BETWEEN :dt1 AND :dt2
       ) v
 WHERE NOT EXISTS (SELECT 1
                     FROM STV202311139__DWH.dm_trans_types tr
                    WHERE tr.trans_type = v.trans_type
	          );
