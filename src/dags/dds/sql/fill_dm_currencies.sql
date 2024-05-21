INSERT INTO STV202311139__DWH.dm_currencies(currency_id, currency_code, load_dt, load_src)
SELECT HASH(v.currency_code) AS currency_id
     , v.currency_code
     , now() AS load_dt
     , 'pg' AS load_src
  FROM (SELECT c.currency_code
          FROM STV202311139__STAGING.currencies c
         WHERE c.date_update = :dt1

         UNION

        SELECT c.currency_code_with
          FROM STV202311139__STAGING.currencies c
         WHERE c.date_update = :dt1

	 UNION

	SELECT t.currency_code
	  FROM STV202311139__STAGING.transactions t
	 WHERE t.transaction_dt BETWEEN :dt1 AND :dt2
       ) v
 WHERE NOT EXISTS (SELECT 1
                     FROM STV202311139__DWH.dm_currencies c
                    WHERE c.currency_code = v.currency_code
                  );
