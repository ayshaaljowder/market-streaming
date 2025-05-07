'''
This python files runs the results Part 4: Optional Bonus
'''

import dash
from dash import dcc, html, dash_table
from dash.dependencies import Input, Output
import plotly.express as px
from threading import Thread
from t2_spark_stream import create_spark, fetch_data_periodically
from t3_visualization import write_output

# dash application creation
app = dash.Dash(__name__)
app.title = "Market Price Analysis Dashboard"

# to change visualization order
column_order = [
    "Index",
    "window_start",
    "avg_price",
    "price_pct_change",
    "price_volatility",
    "volatility_alert"
]

# html report layout
app.layout = html.Div(
    style={"fontFamily": "Roboto"},
    children = [
    html.H1("Market Price Analysis Dashboard", style={"textAlign": "center", "fontWeight": "bold"}),

    # interval component to trigger data update every 5 mins
    dcc.Interval(
        id="interval-component",
        interval=5*60*1000,  # 60 seconds
        n_intervals=0
    ),
    
    html.H2("Market Summary (Latest Window [5 Minutes])", style={"marginTop": "20px", "fontWeight": "bold"}),
    html.H3("- % Prices Up/Down from Last Window"),
    html.H3("- Volatility Spikes from Last Window"),    

    dash_table.DataTable(
        id="price-table",
        columns=[{"name": col, "id": col} for col in column_order if col != "volatility_alert"],
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
    dcc.Graph(id="price-chart"),

    # plot prices up down for last window
    html.H2("% Price Up/Down", style={"fontWeight": "bold"}),
    dcc.Graph(id="heatmap"),

    # plot price volatility for last window
    html.H2("Price Volatility Over Time", style={"fontWeight": "bold"}),
    dcc.Graph("vol-chart")
    ])


# callback function to update the dashboard with new data
@app.callback(
    [Output("price-table", "data"),
     Output("price-chart", "figure"),
     Output("heatmap", "figure")],
     Output("vol-chart", "figure"),
     Input("interval-component", "n_intervals")
)
def update_dashboard(n):
    # read the latest data from parquet file
    try:
        df = spark.read.parquet(output_file).toPandas() 
        if df.empty:
            return [], {}, {}  
    except Exception as e:
        print(f"Error reading data: {e}")
        return [], {}, {}  
    
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
    latest_df = latest_df[column_order]

    # line chart: avg_price over time
    price_fig = px.line(
        df,
        x="window_start",
        y="avg_price",
        color="Index"
    )

    # heatmap: price_pct_change over time
    heatmap_fig = px.imshow(
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

    # heatmap: price_volatility over time
    vol_fig = px.line(
        df.sort_values(by=['Index', 'window_start']),
        x="window_start",
        y="price_volatility",
        color="Index",
    )
    
    # return data to be displayed in the table & graphs
    return latest_df.to_dict("records"), price_fig, heatmap_fig, vol_fig

if __name__ == '__main__':
    output_file = 'spark_output'
    spark, schema = create_spark()
    # simulating yfinance streaming, reading spark aggregations and outputting spark data as parquet
    fetch_thread = Thread(target=fetch_data_periodically)
    fetch_thread.daemon = True
    fetch_thread.start()   
    write_thread = Thread(target=write_output, args=(spark, schema, output_file))  # Pass function with args
    write_thread.daemon = True
    write_thread.start()
    
    # run the app to be updated with the above streamed data
    app.run(debug=True, use_reloader=False)  