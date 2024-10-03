import os
from pyspark.sql import SparkSession
from azure.storage.blob import BlobServiceClient

user_name = os.environ.get('USER_NAME', 'spark')
user_home = os.environ.get('USER_HOME', '/home/spark')
account_key = os.environ.get('account_key')

# Construir a sessão Spark
spark = (SparkSession.builder 
    .appName("JupyterSparkExample") 
    .master("spark://spark-master:7077")
    .config("spark.jars.packages", "org.apache.hadoop:hadoop-azure:3.4.0,com.microsoft.azure:azure-storage:8.6.4")
    .config("spark.hadoop.fs.azure", "org.apache.hadoop.fs.azure.NativeAzureFileSystem")
    .config("spark.hadoop.fs.azure.account.key.rcblob01.blob.core.windows.net", account_key)
    .getOrCreate())


# Azure Blob Storage access info
blob_account_name = "azureopendatastorage"
blob_container_name = "nyctlc"
blob_relative_path = "yellow"

# Construct connection path
wasbs_path = f'wasbs://{blob_container_name}@{blob_account_name}.blob.core.windows.net/{blob_relative_path}'
print(wasbs_path)

# Read parquet data from Azure Blob Storage path
blob_df = spark.read.parquet(wasbs_path)
# blob_df.show()

df = blob_df.select('vendorID').limit(10)

(df.coalesce(2)
 .write
 .format("parquet")
 .mode("append")
 .save("wasbs://read-zone@rcblob01.blob.core.windows.net/silver_b"))

