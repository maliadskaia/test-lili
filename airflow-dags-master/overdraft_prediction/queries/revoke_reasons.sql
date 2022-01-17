/*
This query shows the reason we revoked our customers
Last revision on 04.01.2022
*/
WITH combined_table
    AS --Part 1: combined table is the part 1. which create in a customer level the all the files data between the two diffrent files:
    -- the production and the new prediction
    (
        SELECT CASE
                   WHEN n.bank_account_id IS NULL THEN c.bank_account_number
                   ELSE n.bank_account_id
                   END           AS bank_account_number,
               c.overdraft_limit,
               c.expiration_date,
               n.expiration_date AS new_expiration,
               n.reason,
               n.prediction      AS new_limit,
               n.DAG_RUN         as CREATED_AT,
               CASE
                   WHEN c.bank_account_number IS NULL THEN 'new'
                   ELSE 'existing'
                   END           AS model_type
        FROM "DWH"."ML_OVERDRAFT_POST_PREDICTION" n
                 FULL OUTER JOIN "LILI_ANALYTICS"."ODS"."OVERDRAFT_LIMITS" c
                                 ON n.bank_account_id = c.bank_account_number
    )
   , overdraft_logic
    AS -- overdraft_logic is the part 2. which adds additonal data in the customer level that are not in the files+create logics about the customers
    ( -- logic such as - who we can change and who dont
        SELECT ct.*,
               CASE
                   WHEN (date(EXPIRATION_DATE) not in (date(CURRENT_DATE()),
                                                       date(dateadd(DAY, 1, current_date())))
                       AND OVERDRAFT_LIMIT > 0) THEN 'unchangeable'
                   ELSE 'changeable'
                   END AS status,
               dw.BANK_ACCOUNT_ID,
               dw.pro_customer,
               dw.ACCOUNT_LEGIT,
               b.CURRENT_BALANCE
        FROM combined_table ct
                 INNER JOIN
             (
                 SELECT DISTINCT ACCOUNT_LEGIT,
                                 BANK_ACCOUNT_ID,
                                 BANK_ACCOUNT_NUMBER,
                                 PRO_CUSTOMER
                 FROM "LILI_ANALYTICS"."ODS"."MYSQL_DW_CUSTOMER_MONTHLY_NEW"
                 where year = year(current_date)
                   and month = month(current_date)
             ) dw
             ON ct.BANK_ACCOUNT_NUMBER = dw.BANK_ACCOUNT_NUMBER
                 INNER JOIN
                 (SELECT * FROM "LILI_ANALYTICS"."ODS"."MYSQL_ACCOUNT_CURRENT_BALANCE") b
                 ON dw.BANK_ACCOUNT_ID = b.BANK_ACCOUNT_ID
    )
SELECT REASON,
       count(DISTINCT BANK_ACCOUNT_NUMBER) AS CUSTOMERS
FROM overdraft_logic
where OVERDRAFT_LIMIT > 0
  AND status = 'changeable'
  AND NEW_LIMIT = 0
GROUP BY 1
ORDER BY 2 DESC;