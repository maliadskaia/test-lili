# from random import randint

# min_number = int(input('Please enter the min number: '))
# max_number = int(input('Please enter the max number: '))

# if (max_number < min_number): 
#   print('Invalid input - shutting down...')
# else:
#   rnd_number = randint(min_number, max_number)
#   print(rnd_number)

# !python -m unittest tests.integration.overdraft_predication.test_overdraft_predication.Test.test_predict_and_post_predict
import os 
import snowflake.connector

# SNOWFLAKE_HOST=os.environ["SNOWFLAKE_HOST"]
# SNOWFLAKE_SCHEMA=os.environ["SNOWFLAKE_SCHEMA"]
# SNOWFLAKE_WAREHOUSE=os.environ["SNOWFLAKE_WAREHOUSE"]
# SNOWFLAKE_USER=os.environ["SNOWFLAKE_USER"]
# SNOWFLAKE_PASSWORD=os.environ["SNOWFLAKE_PASSWORD"] 
# SNOWFLAKE_DATABASE=os.environ["SNOWFLAKE_DATABASE"]
SNOWFLAKE_OUTPUT_HOST=os.environ["SNOWFLAKE_OUTPUT_HOST"]
SNOWFLAKE_OUTPUT_USER=os.environ["SNOWFLAKE_OUTPUT_USER"]
SNOWFLAKE_OUTPUT_PASSWORD=os.environ["SNOWFLAKE_OUTPUT_PASSWORD"]
SNOWFLAKE_OUTPUT_SCHEMA=os.environ["SNOWFLAKE_OUTPUT_SCHEMA"]
SNOWFLAKE_OUTPUT_WAREHOUSE=os.environ["SNOWFLAKE_OUTPUT_WAREHOUSE"]
SNOWFLAKE_OUTPUT_DATABASE=os.environ["SNOWFLAKE_OUTPUT_DATABASE"]

conn = snowflake.connector.connect(
  user=os.environ["SNOWFLAKE_USER"],
  password=os.environ["SNOWFLAKE_PASSWORD"],
  account=os.environ["SNOWFLAKE_HOST"],
  warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
  database=os.environ["SNOWFLAKE_DATABASE"],
  schema=os.environ["SNOWFLAKE_SCHEMA"]
  )

for i in conn.cursor().execute("SHOW TABLES"):
  for j in i:
    print(j)