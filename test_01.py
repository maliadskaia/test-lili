import os, boto3
# import snowflake.connector

# SNOWFLAKE_OUTPUT_HOST=os.environ["SNOWFLAKE_OUTPUT_HOST"]
# SNOWFLAKE_OUTPUT_USER=os.environ["SNOWFLAKE_OUTPUT_USER"]
# SNOWFLAKE_OUTPUT_PASSWORD=os.environ["SNOWFLAKE_OUTPUT_PASSWORD"]
# SNOWFLAKE_OUTPUT_SCHEMA=os.environ["SNOWFLAKE_OUTPUT_SCHEMA"]
# SNOWFLAKE_OUTPUT_WAREHOUSE=os.environ["SNOWFLAKE_OUTPUT_WAREHOUSE"]
# SNOWFLAKE_OUTPUT_DATABASE=os.environ["SNOWFLAKE_OUTPUT_DATABASE"]

# conn = snowflake.connector.connect(
#   user=os.environ["SNOWFLAKE_USER"],
#   password=os.environ["SNOWFLAKE_PASSWORD"],
#   account=os.environ["SNOWFLAKE_HOST"],
#   warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
#   database=os.environ["SNOWFLAKE_DATABASE"],
#   schema=os.environ["SNOWFLAKE_SCHEMA"]
#   )

# for i in conn.cursor().execute("SHOW TABLES"):
#   for j in i:
#     print(j)


# s3_resource = boto3.resource('s3',
#   aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
#   aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
#   region_name=os.environ["AWS_REGION"]
#   )

# s3_bucket = s3_resource.Bucket("terraform-nastya-test-bucket")
# files = s3_bucket.objects.all()
# for file in files:
#   print(file)
 
session = boto3.Session( 
    aws_access_key_id=os.environ.get("AWS_ACCESS_KEY_ID"),
    aws_secret_access_key=os.environ.get("AWS_SECRET_ACCESS_KEY"),
    region_name=os.environ["AWS_REGION"]
)

s3 = session.resource('s3')
my_bucket = s3.Bucket('terraform-nastya-test-bucket')

for my_bucket_object in my_bucket.objects.all():
    print(my_bucket_object.key)