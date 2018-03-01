# Databricks notebook source
#--Accessing data from datalake store, E.g. storebotdatalakestore. Ref: https://docs.azuredatabricks.net/spark/latest/data-sources/azure/azure-datalake.html 
#--Note: I need to create a service principal manually from AAD, AD App registration for this databricks
spark.conf.set("dfs.adls.oauth2.access.token.provider.type", "ClientCredential")
spark.conf.set("dfs.adls.oauth2.client.id", "55f2dd64-39d7-4fa5-a12b-5da3bf6e1d50")
spark.conf.set("dfs.adls.oauth2.credential", "yeZZZBYVlWvdbAhPA4EulPtNrlai0GZwdb6vOoHwl8Q=")
spark.conf.set("dfs.adls.oauth2.refresh.url", "https://login.microsoftonline.com/4dced229-4c95-476d-b76b-34d306d723eb/oauth2/token")

df = spark.read.parquet("adl://storebotdatalakestore.azuredatalakestore.net/clusters/sparkcluster")
dbutils.fs.ls("adl://storebotdatalakestore.azuredatalakestore.net/clusters/sparkcluster")
#--Note: I got error "....Could not read footer for file" ??? so I swtich to below mount method

# COMMAND ----------

#--Mounting Azure Data Lake Stores with DBFS (Databricks File System)
configs = {"dfs.adls.oauth2.access.token.provider.type": "ClientCredential",
           "dfs.adls.oauth2.client.id": "55f2dd64-39d7-4fa5-a12b-5da3bf6e1d50",
           "dfs.adls.oauth2.credential": "yeZZZBYVlWvdbAhPA4EulPtNrlai0GZwdb6vOoHwl8Q=",
           "dfs.adls.oauth2.refresh.url": "https://login.microsoftonline.com/4dced229-4c95-476d-b76b-34d306d723eb/oauth2/token"}

dbutils.fs.mount(source = "adl://storebotdatalakestore.azuredatalakestore.net/clusters/sparkcluster", 
                 mount_point = "/mnt/clusters/sparkcluster",
                 extra_configs = configs)
#--Note:if you see extra_configs param error, switch the runtime to 4.0 by creating a new cluster(standard allow you to spefic the rutime version, bot the serverless)

# COMMAND ----------

#--SKIP this step unless you want to unmount a mount point
dbutils.fs.unmount("/mnt/clusters/sparkcluster")

# COMMAND ----------

#--Read the CSV
GermanCreditUCIdata = sqlContext.read.format('csv').options(header='true', inferSchema='true').load('/mnt/clusters/sparkcluster/GermanCreditUCIdataset.csv')
display(GermanCreditUCIdata)

# COMMAND ----------

GermanCreditUCIdata.printSchema()

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TEMPORARY TABLE temp_GermanCreditUCIdataTable  
# MAGIC   USING com.databricks.spark.csv 
# MAGIC   OPTIONS (path '/mnt/clusters/sparkcluster/GermanCreditUCIdataset.csv', header "true")
# MAGIC --Create a temporart table, only in this session

# COMMAND ----------

# MAGIC %sql 
# MAGIC SELECT * FROM temp_GermanCreditUCIdataTable

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE GermanCreditUCIdataTable 
# MAGIC   USING com.databricks.spark.csv 
# MAGIC   AS SELECT ProfileID, CreditHistory, CreditAmount, OtherDebtorsGuarantors, AgeInYears, ForeignWorker, CreditRisk FROM temp_GermanCreditUCIdataTable
# MAGIC --Create a global table wich selected column.  Databricks registers global tables to the Hive metastore and makes them available across all clusters

# COMMAND ----------

# MAGIC %sql
# MAGIC SELECT * FROM GermanCreditUCIdataTable WHERE CreditRisk=2

# COMMAND ----------

# MAGIC %sql
# MAGIC CREATE TABLE IF NOT EXISTS GermanCreditUCIdataTable_Ext 
# MAGIC   STORED AS PARQUET
# MAGIC   LOCATION '/mnt/clusters/sparkcluster/Ext_Table'
# MAGIC   AS SELECT ProfileID, CreditHistory, CreditAmount, OtherDebtorsGuarantors, AgeInYears, ForeignWorker, CreditRisk FROM temp_GermanCreditUCIdataTable
# MAGIC --This external table store at ADL://storebotdatalakestore/clusters/sparkcluster/Ext_Table
# MAGIC --When the cluster is terminated, both GermanCreditUCIdataTable & GermanCreditUCIdataTable_Ext are gone but bpth can resore when the cluster up again.  However the datlakestore still have the table stored at Ext_Table folder in binary format
# MAGIC --Only temp_GermanCreditUCIdataTable not able to restore as its a temporary table. 
# MAGIC 
# MAGIC --df.write.format("parquet").saveAsTable("GermanCreditUCIdataTable_Ext_Perm")?????????

# COMMAND ----------

# MAGIC %sql
# MAGIC SHOW TABLES

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS temp_GermanCreditUCIdataTable

# COMMAND ----------

# MAGIC %sql DROP TABLE IF EXISTS GermanCreditUCIdataTable