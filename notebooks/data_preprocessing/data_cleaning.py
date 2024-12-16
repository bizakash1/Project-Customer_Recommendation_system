# Databricks notebook source
# MAGIC %md
# MAGIC ## Load Data From ADLS

# COMMAND ----------

# conection using SAS token

spark.conf.set("fs.azure.account.auth.type.customerrecommendation.dfs.core.windows.net", "SAS")

spark.conf.set("fs.azure.sas.token.provider.type.customerrecommendation.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider")

spark.conf.set("fs.azure.sas.fixed.token.customerrecommendation.dfs.core.windows.net", "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-12-22T20:03:31Z&st=2024-12-15T12:03:31Z&spr=https&sig=pqi0mBGjVabhu%2FMwjDzM9sPliddEr%2BJG5zWTu3CYO30%3D")

# COMMAND ----------

# show directories

display(dbutils.fs.ls("abfss://raw@customerrecommendation.dfs.core.windows.net/"))

# COMMAND ----------

adls_path = "abfss://raw@customerrecommendation.dfs.core.windows.net/snowflake_data.parquet/"

# COMMAND ----------

# read data
df = spark.read.option("mergeSchema", "true").parquet(adls_path)
display(df)

# COMMAND ----------

type(df)

# COMMAND ----------

df.printSchema()

# COMMAND ----------

# Count total rows and columns
print(f"Rows: {df.count()}, Columns: {len(df.columns)}")

# COMMAND ----------

# Check for missing values
from pyspark.sql.functions import col, sum
display(df_new.select([sum(col(c).isNull().cast("int")).alias(c) for c in df_new.columns]))

# COMMAND ----------

from data.connections_read import make_connection , read_data

# COMMAND ----------

make_connection()

# COMMAND ----------

df_new = read_data(dataframetype="parquet" , path="abfss://raw@customerrecommendation.dfs.core.windows.net/snowflake_data.parquet/")

# COMMAND ----------

display(df_new)

# COMMAND ----------

type(df_new)

# COMMAND ----------

# check skewness

from pyspark.sql.functions import skewness

numerical_columns = [
    "AGE", "FLIGHT_COUNT", "BP_SUM", "SUM_YR_1", "SUM_YR_2", "SEG_KM_SUM", 
    "LAST_TO_END", "AVG_INTERVAL", "MAX_INTERVAL", "EXCHANGE_COUNT", 
    "AVG_DISCOUNT", "POINTS_SUM", "POINT_NOTFLIGHT"
]

for col_name in numerical_columns:
    skew = df_new.select(skewness(col_name)).collect()[0][0]
    print(f"Skewness of {col_name}: {skew}")


# COMMAND ----------

from pyspark.sql.functions import skewness, mean
import pandas as pd

# Select numerical columns
numerical_columns = [
    "AGE", "FLIGHT_COUNT", "BP_SUM", "SUM_YR_1", "SUM_YR_2", 
    "SEG_KM_SUM", "LAST_TO_END", "AVG_INTERVAL", 
    "MAX_INTERVAL", "EXCHANGE_COUNT", "AVG_DISCOUNT", 
    "POINTS_SUM", "POINT_NOTFLIGHT"
]

# Calculate skewness for numerical columns
skewness_values = {
    col_name: df_new.select(skewness(col_name)).collect()[0][0] 
    for col_name in numerical_columns
}

# Convert skewness results to a DataFrame for visualization
skewness_df = pd.DataFrame(list(skewness_values.items()), columns=["Column", "Skewness"])
print(skewness_df)


# COMMAND ----------

import matplotlib.pyplot as plt
import seaborn as sns

# Plot skewness
plt.figure(figsize=(12, 6))
sns.barplot(data=skewness_df, x="Column", y="Skewness", palette="viridis")
plt.xticks(rotation=45, ha="right")
plt.title("Skewness of Numerical Columns")
plt.ylabel("Skewness Value")
plt.xlabel("Column Name")
plt.axhline(y=0, color="r", linestyle="--", linewidth=1, label="Symmetrical")
plt.legend()
plt.tight_layout()
plt.show()


# COMMAND ----------

# Collect a specific column to Pandas for visualization
import pandas as pd

column_to_plot = "FLIGHT_COUNT"  # Replace with any numerical column
data = df_new.select(column_to_plot).toPandas()

# Plot the histogram
plt.figure(figsize=(8, 5))
sns.histplot(data[column_to_plot], kde=True, bins=30, color="blue")
plt.title(f"Distribution of {column_to_plot}")
plt.xlabel(column_to_plot)
plt.ylabel("Frequency")
plt.show()


# COMMAND ----------

# Drop rows with any NULL values
df_cleaned = df_new.dropna()

# Verify no NULL values remain
display(df_cleaned.select([
    sum(col(c).isNull().cast("int")).alias(c) for c in df_cleaned.columns
]))

# COMMAND ----------

# Count total rows and columns
print(f"Rows: {df_cleaned.count()}, Columns: {len(df_cleaned.columns)}")

# COMMAND ----------

df_cleaned.write.format("parquet").mode("overwrite").save("abfss://raw@customerrecommendation.dfs.core.windows.net/cleaned_data_na_removed/")

# COMMAND ----------

display(dbutils.fs.ls("abfss://raw@customerrecommendation.dfs.core.windows.net/"))

# COMMAND ----------


