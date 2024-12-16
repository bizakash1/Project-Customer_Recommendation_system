# Databricks notebook source
from data.connections_read import make_connection , read_data

# COMMAND ----------

make_connection()

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@customerrecommendation.dfs.core.windows.net/"))

# COMMAND ----------

df_cleaned = read_data("parquet" , "abfss://raw@customerrecommendation.dfs.core.windows.net/cleaned_data_na_removed/")

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

display(df_cleaned.toPandas().duplicated())

# COMMAND ----------

# Get a boolean Series indicating duplicate rows
duplicate_rows = df_cleaned.toPandas().duplicated()

# Filter the DataFrame to keep only duplicate rows
duplicate_rows_df = df_cleaned.toPandas()[duplicate_rows]

# Display the duplicate rows
print(duplicate_rows_df)

# COMMAND ----------

duplicate_rows_df

# COMMAND ----------

# MAGIC %md
# MAGIC ##  Standardization

# COMMAND ----------

from pyspark.sql.functions import col

# convert all columns names to the lowercase and if space replace it with underscore
df_cleaned = df_cleaned.select(
    [col(c).alias(c.strip().lower().replace(" ", "_").replace("-", "_")) for c in df_cleaned.columns]
)

# Display the standardized column names
print("Standardized column names:", df_cleaned.columns)

# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

# Standardize date/time formats

from pyspark.sql.functions import to_date

# Convert date columns to a standard format (e.g., 'YYYY-MM-DD')
df_cleaned = df_cleaned.withColumn("ffp_date", to_date(col("ffp_date"), "yyyy-MM-dd"))
df_cleaned = df_cleaned.withColumn("first_flight_date", to_date(col("first_flight_date"), "yyyy-MM-dd"))
df_cleaned = df_cleaned.withColumn("last_flight_date", to_date(col("last_flight_date"), "yyyy-MM-dd"))




# COMMAND ----------

display(df_cleaned)

# COMMAND ----------

df_cleaned.write.format("parquet").mode("overwrite").save("abfss://processed@customerrecommendation.dfs.core.windows.net/final_processed_data/")

# COMMAND ----------


