# Databricks notebook source

# load data from snowflake 

snowflake_table = (spark.read
  .format("snowflake")
  .option("host", "jy33707.central-india.azure.snowflakecomputing.com")
  .option("port", "443") # Optional - will use default port 443 if not specified.
  .option("user", "Akash1106")
  .option("password", "Akion@@11062001")
  .option("sfWarehouse", "COMPUTE_WH")
  .option("database", "TEST_DB")
  .option("schema", "CUSTOMER") # Optional - will use default schema "public" if not specified.
  .option("dbtable", "FLIGHT")
  .load()
)

# COMMAND ----------

# store data in dbfs/mnt

snowflake_table.write.mode("overwrite").save("/mnt/data/raw/flight")

# COMMAND ----------

raw_folder_path = "/dbfs/data/raw/"

# Save the DataFrame as a Parquet file
snowflake_table.write.mode("overwrite").parquet(f"{raw_folder_path}/snowflake_data.parquet")

# Alternatively, save as a CSV file
snowflake_table.write.mode("overwrite").csv(f"{raw_folder_path}/snowflake_data.csv", header=True)

print(f"Data saved to {raw_folder_path}")

# COMMAND ----------

# store data in ADLS blob storage

spark.conf.set("fs.azure.account.auth.type.customerrecommendation.dfs.core.windows.net", "SAS")

spark.conf.set("fs.azure.sas.token.provider.type.customerrecommendation.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

spark.conf.set("fs.azure.sas.fixed.token.customerrecommendation.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-12-22T20:03:31Z&st=2024-12-15T12:03:31Z&spr=https&sig=pqi0mBGjVabhu%2FMwjDzM9sPliddEr%2BJG5zWTu3CYO30%3D")

# COMMAND ----------

# Define the ADLS path
adls_path = "abfss://raw@customerrecommendation.dfs.core.windows.net/"

# Save the DataFrame as a Parquet file
snowflake_table.write.mode("overwrite").parquet(f"{adls_path}/snowflake_data.parquet")

# Alternatively, save as a CSV file
snowflake_table.write.mode("overwrite").csv(f"{adls_path}/snowflake_data.csv", header=True)

print(f"Data saved to {adls_path}")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@customerrecommendation.dfs.core.windows.net/"))

# COMMAND ----------


