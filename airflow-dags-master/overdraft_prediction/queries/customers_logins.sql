SELECT bank_account_id,
       SUM(ZEROIFNULL(login_count)) AS login_count
    FROM   (SELECT bank_account_id,
                   customer_id
            FROM   dwh.fact_mysql_customer_monthly
            GROUP  BY 1,
                      2) bank_to_customer
    LEFT JOIN (SELECT customer_id,
                     COUNT(DISTINCT id) AS login_count
              FROM   "LILI_ANALYTICS"."ODS"."MYSQL_CUSTOMER_LOGIN"
              WHERE  create_time <= %(dag_run_start_date)s
                     AND create_time >= DATEADD(MONTH, -1, DATE(%(dag_run_start_date)s))
              GROUP  BY customer_id) customers_login
          ON bank_to_customer.customer_id = customers_login.customer_id
    GROUP BY 1