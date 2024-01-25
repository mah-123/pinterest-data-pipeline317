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

# AWS S3 bucket name
AWS_S3_BUCKET = "user-0af64aa61d45-bucket"
# Mount name for the bucket
MOUNT_NAME = "/mnt/aws_mount"
# Source url
SOURCE_URL = "s3n://{0}:{1}@{2}".format(ACCESS_KEY, ENCODED_SECRET_KEY, AWS_S3_BUCKET)
# Mount the drive
dbutils.fs.mount(SOURCE_URL, MOUNT_NAME)

# COMMAND ----------

# This is to see if the S3 bucket is mounted correctly
display(dbutils.fs.ls("/mnt/aws_mount/topics"))

# COMMAND ----------

# MAGIC %sql
# MAGIC /*Disable format checks during the reading of Delta tables*/
# MAGIC SET spark.databricks.delta.formatCheck.enabled=false

# COMMAND ----------

#dataframe for pin topic
# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/aws_mount/topics/0af64aa61d45.pin/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_pin = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_pin)

# COMMAND ----------

#dataframe for geo topic
# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/aws_mount/topics/0af64aa61d45.geo/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_geo = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_geo)

# COMMAND ----------

# dataframe for user topic
# File location and type
# Asterisk(*) indicates reading all the content of the specified file that have .json extension
file_location = "/mnt/aws_mount/topics/0af64aa61d45.user/partition=0/*.json" 
file_type = "json"
# Ask Spark to infer the schema
infer_schema = "true"
# Read in JSONs from mounted S3 bucket
df_user = spark.read.format(file_type) \
.option("inferSchema", infer_schema) \
.load(file_location)
# Display Spark dataframe to check its content
display(df_user)

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

# COMMAND ----------

display(df_pin)

# COMMAND ----------

# Make the latitude and longitude as one column array.
df_geo = df_geo.withColumn("coordinates", array("latitude", "longitude"))

# Once the new column coordinates is formed, the previous column can be removed/droped from the dataframe.
df_geo = df_geo.drop("latitude", "longitude")

#This transformation convert a string to a timestamp data type.
df_geo = df_geo.withColumn("timestamp", to_timestamp("timestamp"))

#Adjust the order of columns in df_geo
df_geo = df_geo.select("ind", "country", "coordinates", "timestamp")

# COMMAND ----------

display(df_geo)

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

# COMMAND ----------

display(df_user)

# COMMAND ----------

# Create a dataframe for category popularity by different country
df_category_country = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"], how="inner")

# Remove unnecessary columns for the datarame
df_category_country = df_category_country.drop("unique_id", "ind", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "coordinates", "timestamp")

# Group the the category and country to each other
df_category_country = df_category_country.groupBy("country", "category").count().withColumnRenamed("count", "category_count")

# Order each category by popularirty by each country
df_category_country = df_category_country.orderBy("category_count", ascending=False)

# Display the result
display(df_category_country)

# COMMAND ----------

# Create a dataframe for category popularity each year
df_category_year = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"], how="inner")

# Remove unnecessary columns for the datarame
df_category_year = df_category_year.drop("unique_id", "ind", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "coordinates", "country")

# Adjust the timestamp to only have the year
df_category_year = df_category_year.select("category", year("timestamp").alias("post_year"))

# Create the dataframe for category_count for each year
df_category_year = df_category_year.groupBy("post_year", "category").count().withColumnRenamed("count", "category_count")

# Ordering the data in category count asceding orde
df_category_year = df_category_year.orderBy("category_count", ascending=False)

# Show the dataframe for df_category_year
display(df_category_year)

# COMMAND ----------

# Create dataframe for most followers for each poster_name and follower count for each country.
#Dataframe for most follower_count by poster_name
df_follower_poster = df_pin.join(df_geo, df_pin["ind"] == df_geo["ind"], how="inner")

# Remove unnecessary columns for the datarame
df_follower_poster = df_follower_poster.drop("unique_id", "ind", "title", "description", "tag_list", "is_image_or_video", "image_src", "save_location", "coordinates", "timestamp", "category")

# Change the order for dataframe
df_follower_poster = df_follower_poster.select("country", "poster_name", "follower_count")

# Change the order of follower_count
df_follower_poster = df_follower_poster.orderBy("follower_count", ascending=False)

# Display the dataframe
display(df_follower_poster)

# COMMAND ----------

# Step 2: make a query with the country with the most follower_count
df_follower_country = df_follower_poster.groupBy("country").agg(sum("follower_count").alias("follower_count"))

# Change the follower_count order from ascending
df_follower_country = df_follower_country.orderBy("follower_count", ascending=False)   

# Display the dataframe for most follower_count by country
display(df_follower_country)

# COMMAND ----------

from pyspark.sql.functions import udf
# Create a dataframe for querying popular category for different age group
df_age_group = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner")

# Drop unnecessary column
df_age_group = df_age_group.drop("ind", "unique_id", "title", "description", "follower_count", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "username", "date_joined")

# Create a group age using a udf functions
age_groups = udf(lambda age: "18-24" if(age >= 18 and age <= 24) else
                 "25-35" if(age >= 25 and age <= 35) else
                 "36-50" if(age >= 36 and age <= 50) else
                 "50+" if(age >= 50) else ""
                 )

# Create a new column for age_groups
df_age_group = df_age_group.withColumn("age_groups", age_groups(df_age_group["age"]))

# Adjust the dataframe to have category, category count and age_group
df_age_group = df_age_group.groupBy("age_groups", "category").count().withColumnRenamed("count", "category_count")

# Chanege the order from acsending  
df_age_group = df_age_group.orderBy("category_count", ascending=False)

# Display the dataframe for df_age_group
display(df_age_group)

# COMMAND ----------

from pyspark.sql.functions import udf, percentile_approx # Add median to the list of imported functions

# Create a dataframe for querying median follower_count for different age group
df_median = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner")

# Drop unnecessary column
df_median = df_median.drop("ind", "unique_id", "title", "description", "category", "poster_name", "tag_list", "is_image_or_video", "image_src", "save_location", "username", "date_joined")

# Create a group age using a udf functions
age_groups = udf(lambda age: "18-24" if(age >= 18 and age <= 24) else
                 "25-35" if(age >= 25 and age <= 35) else
                 "36-50" if(age >= 36 and age <= 50) else
                 "50+" if(age >= 50) else ""
                 )

# Create a new column for age_groups
df_median = df_median.withColumn("age_groups", age_groups(df_median["age"]))

# Compute median follower count by age group
df_median_inner = df_median.groupBy("age_groups").agg(sum("follower_count").alias("sum_follower_count"))
df_median_outer = df_median_inner.groupBy("age_groups").agg(percentile_approx(col("sum_follower_count"), 0.5).alias("median_follower_count"))

# Order by median follower count in descending order
df_median_outer = df_median_outer.orderBy("median_follower_count", ascending=False)

# Display the dataframe for df_age_group
display(df_median_outer)

# COMMAND ----------

# Create a dataframe for post_user joined from certain year 2015-2020
df_post_year_count = df_user.select("user_name" , year("date_joined").alias("post_year"))

# Create a column for number of users joined 
df_post_year_count = df_post_year_count.groupBy("post_year").count().withColumnRenamed("count", "number_user_joined")

# Change the order for number_user_joined column
df_post_year_count = df_post_year_count.orderBy("number_user_joined", ascending=False)

# Display the post_year_count dataframe query
display(df_post_year_count)

# COMMAND ----------

# Creating a dataframe for querying post_year jointed for median follower_count
df_post_year_median = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner")

# Select necessary columns
df_post_year_median = df_post_year_median.select(year("date_joined").alias("post_year"), "follower_count")

# Compute median follower count by post_year
df_median_year_inner = df_post_year_median.groupBy("post_year").agg(sum("follower_count").alias("sum_follower_count"))
df_median_year_outer = df_median_year_inner.groupBy("post_year").agg(percentile_approx(col("sum_follower_count"), 0.5).alias("median_follower_count"))

# Adjust the order for median_follower_count
df_median_year_outer = df_median_year_outer.orderBy("median_follower_count", acsending=False)

# Display the dataframe query for post_year_median
display(df_median_year_outer)

# COMMAND ----------

# Create a dataframe for median follower_count based on date_joined and age groups
df_post_median_count = df_pin.join(df_user, df_pin["ind"] == df_user["ind"], how="inner")


# Dataframe joined from user and pin information
df_post_median_count = df_post_median_count.select("age", year("date_joined").alias("post_year"), "follower_count")

# Create a group age using a udf functions
age_groups = udf(lambda age: "18-24" if(age >= 18 and age <= 24) else
                 "25-35" if(age >= 25 and age <= 35) else
                 "36-50" if(age >= 36 and age <= 50) else
                 "50+" if(age >= 50) else ""
                 )

# Create a new column for age_groups
df_post_median_count = df_post_median_count.withColumn("age_groups", age_groups(df_post_median_count["age"]))

# Drop the old age column
df_post_median_count = df_post_median_count.drop('age')

# Group each column by post_year and median_follower count to age_group
# Compute median follower count by age group
df_median_in = df_post_median_count.groupBy("age_groups", "post_year").agg(sum("follower_count").alias("sum_follower_count"))
df_median_out = df_median_in.groupBy("age_groups", "post_year").agg(percentile_approx(col("sum_follower_count"), 0.5).alias("median_follower_count"))

# Adjust the order of ascending
df_median_out = df_median_out.orderBy("median_follower_count", asecending=False)

# Display dataframe
display(df_median_out)

# COMMAND ----------

# Once query had been completed unmount from the bucket
dbutils.fs.unmount("/mnt/aws_mount")

# COMMAND ----------


