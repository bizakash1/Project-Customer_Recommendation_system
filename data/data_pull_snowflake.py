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

# store data in raw/

raw_folder_path = "/data/raw/"

# Save the DataFrame as a Parquet file
snowflake_table.write.mode("overwrite").parquet(f"{raw_folder_path}/snowflake_data.parquet")

# Alternatively, save as a CSV file
snowflake_table.write.mode("overwrite").csv(f"{raw_folder_path}/snowflake_data.csv", header=True)

# COMMAND ----------


