from pyspark.sql import SparkSession
# Create a Spark session
spark = SparkSession.builder.appName("CustomerRecommendationSystem").getOrCreate()


def make_connection():

    """
    make connection with ADLS
    """

    try:
        # Setting the configuration for Azure Data Lake with SAS authentication
        spark.conf.set(
            "fs.azure.account.auth.type.customerrecommendation.dfs.core.windows.net",
            "SAS"
        )

        spark.conf.set(
            "fs.azure.sas.token.provider.type.customerrecommendation.dfs.core.windows.net",
            "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider"
        )

        spark.conf.set(
            "fs.azure.sas.fixed.token.customerrecommendation.dfs.core.windows.net",
            "sv=2022-11-02&ss=bfqt&srt=sco&sp=rwdlacupyx&se=2024-12-22T20:03:31Z&st=2024-12-15T12:03:31Z&spr=https&sig=pqi0mBGjVabhu%2FMwjDzM9sPliddEr%2BJG5zWTu3CYO30%3D"
        )

        return "Connection established successfully."
    
        display(dbutils.fs.ls("abfss://raw@customerrecommendation.dfs.core.windows.net/"))

    except Exception as e:
        return f"An error occurred: {str(e)}"



def read_data(dataframetype, path):

    """
    read data from ADLS
    dataframetype: csv, parquet
    """

    if dataframetype == "csv":
        return spark.read.option("header", "true").csv(path)
    elif dataframetype == "parquet":
        return spark.read.option("mergeSchema", "true").parquet(path)
    else: 
        return "Invalid dataframetype"
    
