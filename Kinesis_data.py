# Databricks notebook source
# pyspark functions
from pyspark.sql.functions import *
# URL processing
import urllib

# COMMAND ----------

# Define the path to the Delta table
delta_table_path = "dbfs:/user/hive/warehouse/authentication_credentials"

# Read the Delta table to a Spark DataFrame
aws_keys_df = spark.read.format("delta").load(delta_table_path)

# COMMAND ----------

# Get the AWS access key and secret key from the spark dataframe
ACCESS_KEY = aws_keys_df.select('Access key ID').collect()[0]['Access key ID']
SECRET_KEY = aws_keys_df.select('Secret access key').collect()[0]['Secret access key']
# Encode the secrete key
ENCODED_SECRET_KEY = urllib.parse.quote(string=SECRET_KEY, safe="")

# COMMAND ----------

# MAGIC %sql
# MAGIC /* Disable format checks during the reading of Delta tables*/
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

df_pin = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0af64aa61d45-pin') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

display(df_pin)

# COMMAND ----------

df_pin = df_pin.selectExpr("CAST(data as STRING)")
display(df_pin)

# COMMAND ----------

df_user = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0af64aa61d45-user') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_user = df_user.selectExpr("CAST(data as STRING)")
display(df_user)

# COMMAND ----------

df_geo = spark \
.readStream \
.format('kinesis') \
.option('streamName','streaming-0af64aa61d45-geo') \
.option('initialPosition','earliest') \
.option('region','us-east-1') \
.option('awsAccessKey', ACCESS_KEY) \
.option('awsSecretKey', SECRET_KEY) \
.load()

# COMMAND ----------

df_geo = df_geo.selectExpr("CAST(data as STRING)")
display(df_geo)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType

# Adjust and converting the dataframe for df_pin
streaming_schema_pin = StructType([
    StructField("index", IntegerType(), True),
    StructField("unique_id", StringType(), True),
    StructField("title", StringType(), True),
    StructField("description", StringType(), True),
    StructField("poster_name", StringType(), True),
    StructField("follower_count", StringType(), True),
    StructField("tag_list", StringType(), True),
    StructField("is_image_or_video", StringType(), True),
    StructField("image_src", StringType(), True),
    StructField("downloaded", StringType(), True),
    StructField("save_location", StringType(), True),
    StructField("category", StringType(), True)
])

df_pin = df_pin.withColumn("data", from_json(col("data"), streaming_schema_pin)).select("data.*")

# COMMAND ----------

from pyspark.sql.functions import regexp_replace
# Cleaning dataframe for df_pin
# .replace uses to place the following error with "User Info Error" with None

df_pin = df_pin.replace({'User Info Error': None}, subset=['follower_count'])
df_pin = df_pin.replace({'User Info Error': None}, subset=['poster_name'])

# This will use the regex_replace to remove the character k and M with the actual value in integer

df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "k", "000"))
df_pin = df_pin.withColumn("follower_count", regexp_replace("follower_count", "M", "000000"))

# Change the datatype for follower_count, index and downloaded column into integer type
df_pin = df_pin.withColumn("follower_count", df_pin["follower_count"].cast("int"))
df_pin = df_pin.withColumn("index", df_pin["index"].cast("int"))
df_pin = df_pin.withColumn("downloaded", df_pin["downloaded"].cast("int")) 

# Adjust the save location column to only have the location path
df_pin = df_pin.withColumn("save_location", regexp_replace("save_location", "Local save in ", ""))

# Rename the column index to ind
df_pin = df_pin.withColumnRenamed("index", "ind")

# Changing the column order for df_pin
df_pin = df_pin.select("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "category")

display(df_pin)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# Adjust and converting the dataframe for df_geo
streaming_schema_geo = StructType([
    StructField("ind", IntegerType(), True),
    StructField("timestamp", TimestampType(), True),
    StructField("latitude", FloatType(), True),
    StructField("longitude", FloatType(), True),
    StructField("country", StringType(), True)
])

df_geo = df_geo.withColumn("data", from_json(col("data"), streaming_schema_geo)).select("data.*")

# COMMAND ----------

# Make the latitude and longitude as one column array.
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Once the new column coordinates is formed, the previous column can be removed/droped from the dataframe.
df_geo = df_geo.drop("latitude", "longitude")

#This transformation convert a string to a timestamp data type.
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

#Adjust the order of columns in df_geo
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

display(df_geo)

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType, TimestampType, FloatType

# Adjust and converting the dataframe for df_user
streaming_schema_user = StructType([
    StructField("ind", IntegerType(), True),
    StructField("first_name", StringType(), True),
    StructField("last_name", StringType(), True),
    StructField("age", IntegerType(), True),
    StructField("date_joined", TimestampType(), True)
])

df_user = df_user.withColumn("data", from_json(col("data"), streaming_schema_user)).select("data.*")

# COMMAND ----------

# Create a column "user_name" which consist of first_name and user_name. 
df_user = df_user.withColumn("user_name", concat("first_name", lit(" "), "last_name"))


# Drop the column for first_name and last_name.
df_user = df_user.drop("first_name", "last_name")


# Convert date_joined as a timestamp.
df_user = df_user.withColumn("date_joined", to_timestamp("date_joined"))

# Convert age to int type
df_user = df_user.withColumn("age", df_user["age"].cast("int"))

# Change the column order for the df_user dataframe.
df_user = df_user.select("ind", "user_name", "age", "date_joined")

display(df_user)

# COMMAND ----------

# Create each tables for geo, user and pin tables to delta
df_pin.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0af64aa61d45_pin_table")

# COMMAND ----------

df_geo.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0af64aa61d45_geo_table")

# COMMAND ----------

df_user.writeStream \
  .format("delta") \
  .outputMode("append") \
  .option("checkpointLocation", "/tmp/kinesis/_checkpoints/") \
  .table("0af64aa61d45_user_table")

# COMMAND ----------


