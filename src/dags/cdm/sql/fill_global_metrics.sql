WITH ds AS (
SELECT fts.transaction_dt::date AS date_update
     , fts.transaction_id
     , fta.account_from
     , fta.amount
     , fta.currency_id
     , dc.currency_code
  FROM STV202311139__DWH.fct_trans_status fts
  JOIN STV202311139__DWH.fct_trans_amount fta
    ON fta.transaction_id = fts.transaction_id
  JOIN STV202311139__DWH.dm_currencies dc
    ON dc.currency_id = fta.currency_id
 WHERE fts.transaction_status = 'done'
   AND fts.transaction_dt BETWEEN :dt1 AND :dt2
),
rp AS (
SELECT d.date_update
     , d.currency_code AS currency_from
     , d.amount * COALESCE(c.currency_with_div, 0) AS amount
     , d.transaction_id
     , d.account_from
  FROM ds d
  LEFT JOIN (SELECT fce.currency_id
                  , fce.currency_with_div
               FROM STV202311139__DWH.fct_currency_exchange fce
               JOIN STV202311139__DWH.dm_currencies dc
                 ON dc.currency_id = fce.currency_id_with
                AND dc.currency_code = 420 
              WHERE fce.update_dt BETWEEN :dt1 AND :dt2
            ) c
    ON c.currency_id = d.currency_id
 WHERE d.currency_code != 420

 UNION ALL

SELECT d.date_update
     , d.currency_code AS currency_from
     , d.amount
     , d.transaction_id
     , d.account_from
  FROM ds d
 WHERE d.currency_code = 420
)
SELECT r.date_update
     , r.currency_from
     , SUM(r.amount) AS amount_total
     , COUNT(DISTINCT r.transaction_id) AS cnt_transactions
     , round(COUNT(r.transaction_id) / COUNT(DISTINCT r.account_from), 2) AS avg_transactions_per_account
     , COUNT(DISTINCT r.account_from) AS cnt_accounts_make_transactions
  FROM rp r
 GROUP BY r.date_update, r.currency_from;
