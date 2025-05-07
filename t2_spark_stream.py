'''
This python files runs the results Part 2: Spark Streaming Pipeline for Assignment 3
'''

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, expr, avg, stddev,coalesce, lit
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType, LongType
from threading import Thread
# this is imported to be used here to simulate spark streaming with incoming data
from t1_data_acq import fetch_data_periodically

# This function aims to create a spark sesssion to be used for different reading/writing purposes
def create_spark():
    # defines the schema for the Parquet data
    schema = StructType([
        StructField("Index", StringType(), True),
        StructField("StreamDatetime", TimestampType(), True),
        StructField("Datetime", StringType(), True),
        StructField("Open", DoubleType(), True),
        StructField("High", DoubleType(), True),
        StructField("Low", DoubleType(), True),
        StructField("Close", DoubleType(), True),
        StructField("Volume", LongType(), True),
        StructField("Dividends", DoubleType(), True),
        StructField("Stock Splits", DoubleType(), True),

    ])

    # create spark session
    spark = SparkSession.builder \
        .appName("finance") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR") # set logs to be appear for errors only
    spark.conf.set("spark.sql.session.timeZone", "Asia/Riyadh") # set as current timezone 

    return spark, schema

# This function reads data from the polled yfinance market data folder
def stream_data(spark, schema):
    # read all subfolders for all yfinance indices
    stream_df = spark.readStream \
        .schema(schema) \
        .option("basePath", "C:\\Users\\aysha\\OneDrive\\Documents\\01_Masters\\Sem 2\\Big Data Analytics\\Assignment 3\\Source Code\\market_data\\") \
        .load("C:\\Users\\aysha\\OneDrive\\Documents\\01_Masters\\Sem 2\\Big Data Analytics\\Assignment 3\\Source Code\\market_data\\*")

    return stream_df

# This function write all polled data into the console
def stream_all_data(spark, schema):
    # reads data
    stream_df = stream_data(spark, schema)
   
    # stream to console
    all = stream_df.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()
    
    all.awaitTermination()

# This perfoms the following aggregations to the read streaming data every min in a 5 min sliding window
# 1. Average price
# 2. Price change percentage with previous window
# 3. Price volatility
def agg_data(spark, schema):
    # reads data
    stream_df = stream_data(spark, schema)
    # apply watermark to handle late data
    stream_df = stream_df.withWatermark("StreamDatetime", "15 minutes")

    # 1 & 3 : aggregate average Close price and standard deviation for volatility  
    agg_df = stream_df.groupBy(
        window(col("StreamDatetime"), "5 minutes", "1 minute"), # 5 min window every 1 min
        col("Index")
    ).agg(
        avg("Close").alias("avg_price"),
        coalesce(stddev("Close"), lit(0)).alias("price_volatility")
    )

    # 2: join current and previous window for price change percentage
    curr = agg_df.alias("curr")
    # shift previous window time to current time
    prev = agg_df.withColumn("shifted_start", col("window.start") + expr("INTERVAL 1 MINUTE")).alias("prev")
    # join the current df and the previous df with shifted time
    joined = curr.join(
        prev,
        (col("curr.Index") == col("prev.Index")) &
        (col("curr.window.start") == expr("prev.shifted_start")),
        "inner"
    )
    # calculate price change from current and previous dfs
    result = joined.select(
        col("curr.window.start").alias("window_start"),
        col("curr.window.end").alias("window_end"),
        col("curr.Index"),
        col("curr.avg_price"),
        col("curr.price_volatility"),
        ((col("curr.avg_price") - col("prev.avg_price")) / col("prev.avg_price") * 100).alias("price_pct_change")
    )

    return result
# This function writes the aggregation to console
def stream_agg_data(spark, schema):
    result = agg_data(spark, schema)
    # output to console
    query = result.writeStream \
        .outputMode("append") \
        .format("console") \
        .option("truncate", False) \
        .start()

    query.awaitTermination()

# start two processes: one for fetching and saving data to simulate streaming, another for streaming 
if __name__ == "__main__":
    spark, schema = create_spark()
    # start both tasks in parallel threads
    fetch_thread = Thread(target=fetch_data_periodically)
    fetch_thread.daemon = True
    fetch_thread.start()
    
    # Uncomment one of them to either 
    # 1. stream_all_data(): stream raw data from yfinance 
    # 2. stream_agg_data(): stream aggregated dat
    #stream_all_data(spark, schema)
    stream_agg_data(spark, schema)  
    