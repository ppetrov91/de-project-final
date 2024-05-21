INSERT INTO STV202311139__DWH.dm_accounts(account_id, account_number, load_dt, load_src)
SELECT HASH(v.account_number) AS account_id
     , v.account_number
     , now() AS load_dt
     , 'pg' AS load_src
  FROM (SELECT t.account_number_from AS account_number
          FROM STV202311139__STAGING.transactions t
         WHERE t.transaction_dt BETWEEN :dt1 AND :dt2
	   AND t.account_number_from > 0

         UNION

        SELECT t.account_number_to AS account_number
          FROM STV202311139__STAGING.transactions t
         WHERE t.transaction_dt BETWEEN :dt1 AND :dt2
	   AND t.account_number_to > 0
       ) v
 WHERE NOT EXISTS (SELECT 1
                     FROM STV202311139__DWH.dm_accounts a
                    WHERE a.account_number = v.account_number
                  );
