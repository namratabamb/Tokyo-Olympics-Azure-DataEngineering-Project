# Databricks notebook source
from pyspark.sql.functions import col
from pyspark.sql.types import IntegerType, DoubleType, BooleanType, DataType

# COMMAND ----------

# clientid = 53783bb1-fd16-4253-a0ba-df7ea74606af
# tenantid = d049763d-2e63-4cc5-95fa-0193e14f217a
# secret-key = IR_8Q~gIienKu3A8BQlJUlT3zyQ61O5H~xNRtcY2
#providing configuration for Databricks to access data from Azure Data lake
configs = {"fs.azure.account.auth.type": "OAuth",
"fs.azure.account.oauth.provider.type": "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider",
"fs.azure.account.oauth2.client.id": "53783bb1-fd16-4253-a0ba-df7ea74606af",
"fs.azure.account.oauth2.client.secret": 'IR_8Q~gIienKu3A8BQlJUlT3zyQ61O5H~xNRtcY2',
"fs.azure.account.oauth2.client.endpoint": "https://login.microsoftonline.com/d049763d-2e63-4cc5-95fa-0193e14f217a/oauth2/token"}


dbutils.fs.mount(
source = "abfss://tokyo-olympic-data@tokyoolympicsdatanb.dfs.core.windows.net", #container@storageaccname
mount_point = "/mnt/tokyoolympic",
extra_configs = configs)
  
     

# COMMAND ----------

# MAGIC %fs
# MAGIC ls "mnt/tokyoolympic"
# MAGIC

# COMMAND ----------

#Initializing spark session built implicitly in Databricks
spark

# COMMAND ----------

#Reading raw data from container in Azure Data Lake
athletes = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/athletes.csv")
coaches = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/coaches.csv")
genders = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolympic/raw-data/genders.csv")
medals = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("/mnt/tokyoolympic/raw-data/medals.csv")
teams = spark.read.format("csv").option("header", "true").load("/mnt/tokyoolympic/raw-data/teams.csv")

# COMMAND ----------

athletes.printSchema()

# COMMAND ----------

coaches.printSchema()

# COMMAND ----------

genders.printSchema()

# COMMAND ----------

teams.printSchema()

# COMMAND ----------

medals.printSchema()

# COMMAND ----------

#transforming data types of csv columns for data integrity and data quality
genders = genders.withColumn("Female", col("Female").cast(IntegerType()))\
    .withColumn("Male",col("Male").cast(IntegerType()))\
        .withColumn("Total", col("Total").cast(IntegerType()))

# COMMAND ----------

genders.printSchema()

# COMMAND ----------

#some query extraction
most_countries_with_gold_medals = medals.orderBy("Gold", ascending = False).select("Team_Country","Gold").show()

# COMMAND ----------

avg_entry_by_gender_per_discipline = genders.withColumn(
    'Avg_Female', genders['Female']/genders['Total']
).withColumn(
    'Avg_Male', genders['Male']/genders['Total']
)

avg_entry_by_gender_per_discipline.show()

# COMMAND ----------

# writing this basic transformed data to Azure Data Lake

athletes.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/transformed-data/athletes")
coaches.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/transformed-data/coaches")
genders.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/transformed-data/genders")
medals.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/transformed-data/medals")
teams.repartition(1).write.mode("overwrite").option("header","true").csv("/mnt/tokyoolympic/transformed-data/teams")

# COMMAND ----------


