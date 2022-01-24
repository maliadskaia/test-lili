/*
This query compared current and previous(which inside prodction) BU status of customers (at prediction_date level).
it is build in 4 steps:
  1.gather all the files(prediction and prodction) data in a customer level - combined_table
  2.create the additonal data(balances/pro/legt) and the set of rules in a customer level - overdraft_logic
  3.create aggregated calculation in a files level - between prediction and production -tests
  4.create the measures that indicate the changes between the files  - measurements
Last revision on 04.01.2022
*/

with combined_table
    AS --Part 1: combined table is the part 1. which create in a customer level the all the files data between the two diffrent files:
    -- the production and the new prediction
    (
        SELECT COALESCE(n.bank_account_id, c.bank_account_number)    AS bank_account_number,
               c.overdraft_limit,
               c.expiration_date,
               n.expiration_date                                     AS new_expiration,
               n.reason,
               n.prediction                                          AS new_limit,
               n.DAG_RUN                                             as CREATED_AT,
               IFF(c.bank_account_number IS NULL, 'new', 'existing') AS model_type
        FROM "DWH"."ML_OVERDRAFT_POST_PREDICTION" n
                 FULL OUTER JOIN "LILI_ANALYTICS"."ODS"."OVERDRAFT_LIMITS" c
                                 ON n.bank_account_id = c.bank_account_number
    )
   , overdraft_logic
    AS -- overdraft_logic is the part 2. which adds additonal data in the customer level that are not in the files+create logics about the customers
    ( -- logic such as - who we can change and who dont
        SELECT ct.*,
               IFF(date(EXPIRATION_DATE) not in (DATE(%(dag_run_start_date)s), DATE(%(dag_run_start_date)s) + 1)
                       AND OVERDRAFT_LIMIT > 0, 'unchangeable', 'changeable') AS status,
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
                 where year = year(DATE(%(dag_run_start_date)s))
                   and month = month(DATE(%(dag_run_start_date)s))
             ) dw
             ON ct.BANK_ACCOUNT_NUMBER = dw.BANK_ACCOUNT_NUMBER
                 INNER JOIN "LILI_ANALYTICS"."ODS"."MYSQL_ACCOUNT_CURRENT_BALANCE" b
                            ON dw.BANK_ACCOUNT_ID = b.BANK_ACCOUNT_ID
    )
   -- SELECT * FROM overdraft_logic
-- );


   --***--***----***--***----***--***----***--***--
   ---** SHOULD BUILD A VIEW OF final_logic ** --
   ------SELECT * FROM final_logic
   --***--***----***--***----***--***----***--***--

   , tests AS -- tests is the part 3. where all the calculation in the file level are made
    (
        SELECT COUNT(DISTINCT
                     CASE WHEN NEW_LIMIT > 0 AND status <> 'unchangeable' THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS new_eligibles_new_prediction,     --*** TEST 1

               COUNT(DISTINCT CASE
                                  WHEN (NEW_LIMIT > 0 AND status <> 'unchangeable') OR
                                       (OVERDRAFT_LIMIT > 0 AND status = 'unchangeable') THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS total_eligibles_new_prediction,   --*** TEST 2

               COUNT(DISTINCT CASE WHEN OVERDRAFT_LIMIT > 0 THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS total_eligibles_prod_prediction,  --*** TEST 3

               COUNT(DISTINCT BANK_ACCOUNT_NUMBER)
                   AS total_customers_new_prediction,   --*** TEST 4

               SUM(CASE WHEN status = 'unchangeable' THEN OVERDRAFT_LIMIT ELSE NEW_LIMIT END)
                   AS total_money_new_predication,      --*** TEST 5

               (
                   SELECT prediction
                   FROM "DWH"."ML_OVERDRAFT_POST_PREDICTION"
                   WHERE prediction > 0
                   GROUP BY 1
                       QUALIFY ROW_NUMBER() OVER ( ORDER BY COUNT(DISTINCT BANK_ACCOUNT_ID) DESC) = 1
               )   AS most_freq_bucket_new_predication, --*** TEST 6

               SUM(OVERDRAFT_LIMIT)
                   AS total_money_prod_predication,     --*** TEST 7

               count(DISTINCT CASE
                                  WHEN (CURRENT_BALANCE < 0 AND NEW_LIMIT > 0 AND model_type = 'new')
                                      THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS new_with_negative_balance,

               count(DISTINCT CASE WHEN OVERDRAFT_LIMIT = 20 THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS bucket_20_prod_prediction,

               count(DISTINCT CASE WHEN OVERDRAFT_LIMIT = 40 THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS bucket_40_prod_prediction,

               count(DISTINCT CASE WHEN OVERDRAFT_LIMIT = 50 THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS bucket_50_prod_prediction,

               count(DISTINCT CASE
                                  WHEN (status = 'unchangeable' AND OVERDRAFT_LIMIT = 20) OR
                                       (NEW_LIMIT = 20 AND status = 'changeable') THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS bucket_20_new_prediction,

               count(DISTINCT CASE
                                  WHEN (status = 'unchangeable' AND OVERDRAFT_LIMIT = 40) OR
                                       (NEW_LIMIT = 40 AND status = 'changeable') THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS bucket_40_new_prediction,

               count(DISTINCT CASE
                                  WHEN (status = 'unchangeable' AND OVERDRAFT_LIMIT = 50) OR
                                       (NEW_LIMIT = 50 AND status = 'changeable') THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS bucket_50_new_prediction,

               count(DISTINCT
                     CASE WHEN (NEW_LIMIT = 20 AND status = 'changeable') THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS new_eligible_bucket_20_new_prediction,

               count(DISTINCT
                     CASE WHEN (NEW_LIMIT = 40 AND status = 'changeable') THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS new_eligible_bucket_40_new_prediction,

               count(DISTINCT
                     CASE WHEN (NEW_LIMIT = 50 AND status = 'changeable') THEN BANK_ACCOUNT_NUMBER ELSE NULL END)
                   AS new_eligible_bucket_50_new_prediction,

               count(DISTINCT CASE
                                  WHEN (OVERDRAFT_LIMIT > 0 AND status = 'changeable' AND NEW_LIMIT = 0)
                                      THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS revoked_new_prediction,

               count(DISTINCT CASE
                                  WHEN (OVERDRAFT_LIMIT > 0 AND NEW_LIMIT = 0 AND NEW_EXPIRATION < DATE(%(dag_run_start_date)s) + 1)
                                      THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS revoked_no_expiration_new_prediction,

               count(DISTINCT CASE
                                  WHEN (OVERDRAFT_LIMIT > 0 AND NEW_LIMIT = 0 AND status = 'unchangeable')
                                      THEN BANK_ACCOUNT_NUMBER
                                  ELSE NULL END)
                   AS revoked_no_expiration_new_prediction_V1,

               count(DISTINCT CASE
                                  WHEN (OVERDRAFT_LIMIT IS NOT NULL AND pro_customer = 1) THEN bank_account_number
                                  ELSE NULL END)
                   AS total_pro_prod_prediction,

               count(DISTINCT
                     CASE WHEN (NEW_LIMIT IS NOT NULL AND pro_customer = 1) THEN bank_account_number ELSE NULL END)
                   AS total_pro_new_prediction,

               (
                   SELECT COUNT(DISTINCT BANK_ACCOUNT_ID)
                   FROM "LILI_ANALYTICS"."ODS"."MYSQL_DW_CUSTOMER_MONTHLY_NEW"
               )
                   AS mysql_customers_cnt,

               count(DISTINCT BANK_ACCOUNT_ID)
                   AS new_prediction_customers_cnt,

               count(DISTINCT CASE WHEN ACCOUNT_LEGIT = 0 AND NEW_LIMIT > 0 THEN bank_account_number ELSE NULL END)
                   AS total_eligible_not_legit_new_prediction,

               (
                   select count(*)
                   from (
                            SELECT count(distinct rev.bank_account_id)
                            FROM (
                                     SELECT DISTINCT BANK_ACCOUNT_ID, CREATED_AT, bank_account_number
                                     FROM overdraft_logic
                                     WHERE (OVERDRAFT_LIMIT > 0 AND status = 'changeable' AND NEW_LIMIT = 0)
                                       AND PRO_CUSTOMER = 1
                                       AND account_legit = 1
                                       and current_balance > 1
                                 ) rev
                                     INNER JOIN
                                 (
                                     SELECT ata.BANK_ACCOUNT_ID, ata.transaction_amount, ata.transaction_date
                                     FROM "LILI_ANALYTICS_DEV"."ODS"."MYSQL_ACCOUNT_TRANSACTION_ALL" ata
                                              INNER JOIN LILI_ANALYTICS.ods.MYSQL_DIRECT_DEPOSIT_SOURCES AS dss
                                                         ON dss.merchant = ata.details
                                     WHERE (((ata.act_type = 'PM' AND ata.type <> 'C2') OR
                                             (ata.act_type = 'AD' AND ata.details = 'Debit Card transfer')
                                         OR (ata.act_type = 'AD' AND ata.type = 'FM' AND
                                             ata.details != 'Direct Deposit Return')) AND ata.transaction_amount > 1)
                                       AND ata.transaction_date >= '2021-05-01'
                                       AND upper(dss.type) in ('PAYROLL', 'PAYMENTS', 'UNEMPLOYMENT', 'MARKETPLACE',
                                                               'SOCIAL SECURITY ADMINISTRATION', 'FEDERAL PAYROLL')
                                 ) ata
                                 ON (rev.BANK_ACCOUNT_ID = ata.BANK_ACCOUNT_ID AND
                                     ata.transaction_date BETWEEN dateadd(MONTH, -1, rev.CREATED_AT) AND rev.CREATED_AT)

                            GROUP BY rev.BANK_ACCOUNT_ID--,rev.bank_account_number
                            HAVING sum(ata.transaction_amount) > 500
                        )
               )
                   AS revoked_RECURRENT_new_predication

        FROM overdraft_logic
    )

   , measurements
    as -- measurements is the part 4. which all the final results are presented -> and on those scores with relevant thershold for each column we will decide to upload the file
    (
        select MYSQL_CUSTOMERS_CNT - NEW_PREDICTION_CUSTOMERS_CNT                   AS GAP_BETWEEN_MYSQL_TO_PREDICTION_ALL_CUSTOMERS,
               0                                                                    AS GAP_BETWEEN_MYSQL_TO_PREDICTION_ALL_CUSTOMERS_THRESHOLD,

               (TOTAL_ELIGIBLES_NEW_PREDICTION / TOTAL_PRO_NEW_PREDICTION) -
               (TOTAL_ELIGIBLES_PROD_PREDICTION / TOTAL_PRO_PROD_PREDICTION)        AS CHANGE_IN_PRO_ELIGIBLE_ABSOLUTE_NUMB,
               0.01                                                                 AS CHANGE_IN_PRO_ELIGIBLE_ABSOLUTE_NUMB_THRESHOLD,
               TOTAL_ELIGIBLES_NEW_PREDICTION / TOTAL_PRO_NEW_PREDICTION            AS CHANGE_IN_PRO_ELIGIBLE_ABSOLUTE_NUMB_VALUE,

               BUCKET_20_NEW_PREDICTION / BUCKET_20_PROD_PREDICTION - 1             AS CHANGE_IN_20,
               0.15                                                                 AS CHANGE_IN_20_THRESHOLD,
               BUCKET_20_NEW_PREDICTION                                             AS CHANGE_IN_20_VALUE,


               BUCKET_40_NEW_PREDICTION / BUCKET_40_PROD_PREDICTION - 1             AS CHANGE_IN_40,
               0.15                                                                 AS CHANGE_IN_40_THRESHOLD,
               BUCKET_40_NEW_PREDICTION                                             AS CHANGE_IN_40_VALUE,

               BUCKET_50_NEW_PREDICTION / BUCKET_50_PROD_PREDICTION - 1             AS CHANGE_IN_50,
               0.15                                                                 AS CHANGE_IN_50_THRESHOLD,
               BUCKET_50_NEW_PREDICTION                                             AS CHANGE_IN_50_VALUE,


               NEW_ELIGIBLE_BUCKET_20_NEW_PREDICTION,
               NEW_ELIGIBLE_BUCKET_40_NEW_PREDICTION,
               NEW_ELIGIBLE_BUCKET_50_NEW_PREDICTION,

               MOST_FREQ_BUCKET_NEW_PREDICATION,
               40                                                                   AS MOST_FREQ_BUCKET_NEW_PREDICATION_THRESHOLD,

               TOTAL_ELIGIBLES_NEW_PREDICTION / TOTAL_ELIGIBLES_PROD_PREDICTION - 1 AS CHANGE_IN_ELIGIBILITY,
               0.15                                                                 as CHANGE_IN_ELIGIBILITY_THRESHOLD,
               TOTAL_ELIGIBLES_NEW_PREDICTION                                       AS CHANGE_IN_ELIGIBILITY_VALUE,


               TOTAL_MONEY_NEW_PREDICATION / TOTAL_MONEY_PROD_PREDICATION - 1       AS MONEY_RATIO,
               0.15                                                                 as MONEY_RATIO_THRESHOLD,
               TOTAL_MONEY_NEW_PREDICATION                                          as MONEY_RATIO_VALUE,

               (NEW_ELIGIBLE_BUCKET_20_NEW_PREDICTION * 20) + (NEW_ELIGIBLE_BUCKET_40_NEW_PREDICTION * 40) +
               (NEW_ELIGIBLE_BUCKET_50_NEW_PREDICTION * 50)                         AS TOTAL_NEW_CREDIT,

               NEW_ELIGIBLES_NEW_PREDICTION                                         AS NEW_ELIGIBLE_PREDICTION,
               1200                                                                 AS NEW_ELIGIBLE_PREDICTION_THRESHOLD,

               NEW_WITH_NEGATIVE_BALANCE,
               0                                                                    AS NEW_WITH_NEGATIVE_BALANCE_THRESHOLD,

               REVOKED_NEW_PREDICTION,
               600                                                                  AS REVOKED_NEW_PREDICTION_THRESHOLD,

               TOTAL_ELIGIBLE_NOT_LEGIT_NEW_PREDICTION,
               0                                                                    AS TOTAL_ELIGIBLE_NOT_LEGIT_NEW_PREDICTION_THRESHOLD,

               REVOKED_RECURRENT_NEW_PREDICATION,
               0                                                                    AS REVOKED_RECURRENT_NEW_PREDICATION_THRESHOLD

        from tests
    )

select *
from measurements
