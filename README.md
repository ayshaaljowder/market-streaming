<h1>Spark Streaming and Global Market Analysis</h1>

This repository hosts the code for a Big Data Analytics Course.

The goal of this assignment is to simulate streaming of market data & prices. This is done through using the Yahoo Finance (yfinance) and Spark Streaming (pyspark) libraries.

An example of the final dashboard generated can be found in <em>example_dashboard.pdf</em>

<h2>Requirements</h2>
Python Version=3.13.2

Packages can be installed using the requirements.txt file by running the following bash script in the python environment.
```python
pip install -r requirements.txt
```

To be able to run Spark locally, the Java Development Kit (JDK) as well as Hadoop binaries need to be downloaded and configured. 

JDK Download: https://www.oracle.com/middleeast/java/technologies/downloads/

Hadoop binaries: https://github.com/cdarlint/winutils 

<h2>Scripts</h2>
The following describes the tasks conducted by each file in this repository:

- <em>t1_data_acq.py</em>: Part 1 of the assignment. This fetches data for 5 global stock indices [Dow Jones (^DJI), NASDAQ Composite (^IXIC), FTSE 100 (^FTSE), Nikkei 225 (^N225), Shanghai Composite (000001.SS)] every 1 min and saves it as parquet in the folder <em>market_data</em>

- <em>t2_spark_stream.py</em>: Part 2 of the assignment. This streams the data being added every 1 min in <em>market_data</em> as a spark streaming object. This script also computes metrics every 1 minute in a 5 minutes window to calculate the following:
    <ol>
    <li>Average Price</li>
    <li>Price Change Percentage</li>
    <li>Price Volatility</li>
    </ol>
    To change between streaming raw data and aggregated data. Uncomment out the functions in the main conditional statements. As an example:

    ```python
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
    ```
- <em>t3_visualization.py</em>: Part 3 of the assignment. This outputs the streamed data in Spark as parquet in the <em>spark_output</em> folder every 5 minutes. The <em>spark_output_checkpoints</em>  folder stores the necessary checkpoints for the writing stream. This script also generates charts to create time-series and heatmaps of average prices. Moreover, a static dash dashboard is generated to highlight the recent window spikes in prices and volatility, as well as display visualizations of average prices, price percentage change and volatility. Similar to t2, desired function should be uncommented. 

- <em>t4_real_time_viz.py</em>: Optional part of the assignment. This script runs the yfinance polling, spark aggregation streaming and a dash dashboard to update the data based on the stream every 5 mins. The content is the same as the one in t3.

 
