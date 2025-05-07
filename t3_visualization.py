'''
This python files runs the results Part 3: Visualization for Assignment 3
'''

from t1_data_acq import fetch_data_periodically # to simulate streaming
from t2_spark_stream import agg_data, create_spark # to perform aggregation and output as files
from threading import Thread
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import seaborn as sns
import dash
from dash import dcc, html, dash_table
import plotly.express as px

# This function calls the aggregated df from Task 2 and outputs it in partitioned parquet every 5 mins
def write_output(spark, schema, output_file):
    stream_df = agg_data(spark, schema)
    
    query = stream_df.writeStream \
    .outputMode("append") \
    .format("parquet") \
    .option("path", output_file)  \
    .partitionBy("Index") \
    .option("checkpointLocation", "spark_output_checkpoint")  \
    .trigger(processingTime='5 minutes')  \
    .start()

    query.awaitTermination()

# This function prints two static charts from the aggregated market data stores in the output folder
def print_charts(spark, output_file):
    df = spark.read.parquet(output_file).toPandas()

    # create pivot table to chart each index as one line
    pivot_df = df.pivot(index="window_start", columns="Index", values="avg_price")
    # sort for time series
    pivot_df.sort_index(inplace=True)

    # print line and heatmaps
    line_chart(pivot_df)
    heatmap(pivot_df)

# This function generates a time series line chart for average prices per index
def line_chart(pivot_df):   
    fig, ax = plt.subplots(figsize=(12, 6), constrained_layout=True)
    pivot_df.plot(ax=ax, marker='o', title="Avg Prices (per 5 mins) Over Time by Index")
    
    # rotate labels for readability
    plt.setp(ax.get_xticklabels(), rotation=45, ha='right')
    # format the x-axis with readable timestamps
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d\n%H:%M'))  
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    # set labels and grid
    ax.set_xlabel("Time")
    ax.set_ylabel("Average Price")
    ax.grid(True)
    # place legend outside the chart
    ax.legend(loc='center left', bbox_to_anchor=(1.0, 0.5))

    plt.show()

# This function generates a heat map for average prices per index
def heatmap(pivot_df):   
    fig, ax = plt.subplots(figsize=(14, 6))
    sns.heatmap(
        pivot_df.T, # transpose so the index is on the y-axis
        cmap='viridis',           
        annot=False, 
        ax=ax,             
        cbar_kws={'label': 'Average Price'}
    )

    # rotate labels for readability
    ax.set_xticklabels(
        pivot_df.index, 
        rotation=45,
        ha='right'
    )
    # format the x-axis with readable timestamps
    ax.xaxis.set_major_formatter(mdates.DateFormatter('%Y-%m-%d %H:%M'))
    ax.xaxis.set_major_locator(mdates.AutoDateLocator())
    ax.grid(True)

    # Label formatting
    plt.title("Heatmap of Avg Price (per 5 mins) Over Time by Index")
    plt.xlabel("Time")
    plt.ylabel("Index")


    plt.tight_layout()
    plt.show()

# This function creates a static dash application summarizing all the key insights from market data
# including markets that are up/down as well as volatility spikes
def dash_app(spark, output_file):
    # reads from spark output files
    df = spark.read.parquet(output_file).toPandas()
    df = df.sort_values(by=['Index', 'window_start'])

    # using latest window for the summary
    latest_time = df['window_start'].max()
    latest_df = df[df['window_start'] == latest_time].copy()
    latest_df.sort_values("price_pct_change", ascending=False, inplace=True)
    # determining a dynamic threshold for top 10% volatility
    latest_df['price_volatility'] = latest_df['price_volatility'].fillna(0)
    threshold = latest_df['price_volatility'].quantile(0.9)
    latest_df['volatility_alert'] = (latest_df['price_volatility'] > threshold).astype(int)

    # pivot data for heat_map
    pivot_change = df.pivot(index="window_start", columns="Index", values="price_pct_change")

    # to change visualization order
    column_order = [
        "Index",
        "window_start",
        "avg_price",
        "price_pct_change",
        "price_volatility",
        "volatility_alert"
    ]
    latest_df = latest_df[column_order]
    # dash application creation
    app = dash.Dash(__name__)
    app.title = "Market Price Analysis Dashboard"

    # html report layout
    app.layout = html.Div(
        style={"fontFamily": "Roboto"},
        children = [
        
        html.H1("Market Price Analysis Dashboard", style={"textAlign": "center", "fontWeight": "bold"}),

        html.H2("Market Summary (Latest Window [5 Minutes])", style={"marginTop": "20px", "fontWeight": "bold"}),
        html.H3("- % Prices Up/Down from Last Window"),
        html.H3("- Volatility Spikes from Last Window"),

        # summary table
        dash_table.DataTable(
            columns=[{"name": col, "id": col} for col in column_order if col != "volatility_alert"],
            data=latest_df.to_dict("records"),
            # style table based on price increase/decrease as well as volatility
            style_data_conditional=[
                {
                    'if': {
                        'filter_query': '{price_pct_change} > 0',
                        'column_id': 'price_pct_change'
                    },
                    'backgroundColor': 'green',  
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                {
                    'if': {
                        'filter_query': '{price_pct_change} < 0',
                        'column_id': 'price_pct_change'
                    },
                    'backgroundColor': '#FF0000',  
                    'color': 'white',
                    'fontWeight': 'bold'
                },
                 # Highlight volatility alert
                {
                    'if': {'filter_query': '{volatility_alert} = 1',
                           'column_id': 'price_volatility' },
                    'backgroundColor': '#FF0000',  
                    'color': 'white',  
                    'fontWeight': 'bold'
                }
            ],
            style_table={'overflowX': 'auto'},
            style_cell={'textAlign': 'left'},
            style_header={'fontWeight': 'bold'},
        ),

        # plot price for each index over time
        html.H2("Average Price Over Time", style={"fontWeight": "bold"}),
        dcc.Graph(
            figure=px.line(
                df,
                x="window_start",
                y="avg_price",
                color="Index"
            )
        ),

        # plot prices up down for last window
        html.H2("% Price Up/Down", style={"fontWeight": "bold"}),
        dcc.Graph(
            figure=px.imshow(
                pivot_change.T,
                aspect="auto",
                color_continuous_scale="RdYlGn",
                zmin=-10,
                zmax=10,
                labels=dict(x="Time", y="Index", color="price_pct_change"),
                x=pivot_change.index.strftime("%Y-%m-%d %H:%M"),
            ).update_layout(
                xaxis_title="Time",
                yaxis_title="Index"
            )
        ),
        # plot price volatility for last window
        html.H2("Price Volatility Over Time", style={"fontWeight": "bold"}),
        dcc.Graph(
            figure=px.line(
            df.sort_values(by=['Index', 'window_start']),
            x="window_start",
            y="price_volatility",
            color="Index",
            )
        )
    ] )

    return app

if __name__ == "__main__":
    output_file = 'spark_output'
    spark, schema = create_spark()
    # uncomment the follwowing based on the desired task:
    # 1. simulating yfinance streaming, reading spark aggregations and outputting spark data as parquet
    # Start both tasks in parallel threads
    # fetch_thread = Thread(target=fetch_data_periodically)
    # fetch_thread.daemon = True
    # fetch_thread.start()
    # write_output(spark, schema, output_file)

    # 2. Printing static visualization charts
    #print_charts(spark, output_file)

    # 3. creating static dash application
    dash_app(spark, output_file).run(debug=True)


