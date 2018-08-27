# Databricks notebook source
# Option 1: Direct access to Blob storage container 
spark.conf.set(
  "fs.azure.account.key.creditrisksparkdemoblob.blob.core.windows.net",
  "bPoQlfoOndT9M3TiCwvGph5H4aFQa066HR6/jt3OWxS6wxFIupvgopzlWSU/LHAF4ftTt8ExB4tJQ4UWdGlTBg==")

#val df = spark.read.parquet("wasbs://creditrisksparkdemocontainer@creditrisksparkdemoblob.blob.core.windows.net/NYCTaxiDatadownload/nycTaxiTripData2013")

dbutils.fs.ls("wasbs://creditrisksparkdemocontainer@creditrisksparkdemoblob.blob.core.windows.net/NYCTaxiData/download/nycTaxiTripData2013")

file_name = "wasbs://creditrisksparkdemocontainer@creditrisksparkdemoblob.blob.core.windows.net/NYCTaxiData/trip_data_1_5k.csv"
file_type = "csv"

# df = spark.read.format(file_type).option("inferSchema", "true").load(file_location)
# OR with Column name inserted
df = spark.read.csv(file_name).toDF('medallion','hack_license','vendor_id', 'rate_code', 'store_and_fwd_flag', 'pickup_datetime', 'dropoff_datetime', 'passenger_count', 'trip_time_in_secs', 'trip_distance', 'pickup_longitude', 'pickup_latitude', 'dropoff_longitude', 'dropoff_latitude')

# COMMAND ----------

# Create a Temporary view of a table
df.createOrReplaceTempView("trip_data_1_5k")

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM trip_data_1_5k

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT COUNT(pickup_datetime), SUM(passenger_count), AVG(trip_time_in_secs), AVG(trip_distance) FROM trip_data_1_5k

# COMMAND ----------

df.take(1)

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS PERMANENT_trip_data_1_5k

# COMMAND ----------

# Write to a permanent table
# Temp table available only to this notebook. If you'd like other users to be able to query this table, you can also create a table from the DataFrame.
df.write.format("hive").saveAsTable("PERMANENT_trip_data_1_5k")

# COMMAND ----------

# Option 2: mount on blob
# Mout the creditriskdemoblob on to the databricks file system for direct access to blob

# Enable Access token from the Account user setting 
# Databricks Assec token ID: 82bd400223ed88886141e9de5d86310d47311c9f3f9cc1ed5cdbab17a68cb4b8
# Databricks Assec token: dapi9c9f1f7a6033c6d1dddbf0991e4be76f
#
# Use databrick restful api https://docs.databricks.com/api/latest/index.html
# with Postman to create the DatabricksDemo CLI Access token, then create Secret scope, then put secret to the secret scope.  the CLI doent work with always complain the toekn is incorrect
  
dbutils.fs.mount(
  source = "wasbs://creditrisksparkdemocontainer@creditrisksparkdemoblob.blob.core.windows.net",
  mount_point = "/mnt/external/",
  extra_configs = {"fs.azure.sas.creditrisksparkdemocontainer.creditrisksparkdemoblob.blob.core.windows.net":dbutils.secrets.get(scope = "databricksdemoscope", key = "databricksecret")})

# COMMAND ----------

3# run this only if you want to unmount the blob
dbutils.fs.unmount("/mnt/NYCTaxiData")

# COMMAND ----------

#Below still in testing.................................

# COMMAND ----------

# Mount issue to blob
#df = sc.textFile("/mnt/NYCTaxiData/")
#df = spark.read.text("/mnt/%s/" % "NYCTaxiData/NYCTaxiData/trip_data_1_5k.csv")
#df = spark.read.text("/mnt/external/NYCTaxiData/trip_data_1_5k.csv")

display(dbutils.fs.ls("file:/dbfs/mnt/external")) #why path not exist?
#display(dbutils.fs.ls("file:/dbfs/mnt/")) 
#display(dbutils.fs.ls("file:/dbfs/mnt/NYCTaxiData"))


# COMMAND ----------

# Direct access trip_data.7z from creditrisksparkdemoblob container 
spark.conf.set(
  "fs.azure.account.key.creditrisksparkdemoblob.blob.core.windows.net",
  "bPoQlfoOndT9M3TiCwvGph5H4aFQa066HR6/jt3OWxS6wxFIupvgopzlWSU/LHAF4ftTt8ExB4tJQ4UWdGlTBg==")

#dbutils.fs.ls("wasbs://creditrisksparkdemocontainer@creditrisksparkdemoblob.blob.core.windows.net/NYCTaxiData/download/nycTaxiTripData2013")
#display(dbutils.fs.ls("wasbs://creditrisksparkdemocontainer@creditrisksparkdemoblob.blob.core.windows.net/NYCTaxiData/download/nycTaxiTripData2013"))

trip_data = "wasbs://creditrisksparkdemocontainer@creditrisksparkdemoblob.blob.core.windows.net/NYCTaxiData/download/nycTaxiTripData2013/trip_data.7z"
display(dbutils.fs.ls(trip_data))

# COMMAND ----------

# https://docs.databricks.com/spark/latest/data-sources/zip-files.html 
# Create an RDD from the zipFile with the newAPIHadoopFile command
zip_file_rdd = sc.newAPIHadoopFile(
        trip_data,
        "com.cotdp.hadoop.ZipFileInputFormat",
        "org.apache.hadoop.io.Text",
        "org.apache.hadoop.io.BytesWritable")

# COMMAND ----------

file_contents_rdd = zip_file_rdd.map(lambda s: s[1]).flatMap(lambda s: s.split("\n"))
lines_rdd = file_contents_rdd.flatMap(lambda s: s.split("\n"))
lines_rdd.take(2)