'''
This python files runs the results Part 1: Data Acquisition for Assignment 3
'''

import yfinance as yf
from curl_cffi import requests
from datetime import datetime
import time
import os
import pandas as pd


# This function emulates polling market data from yfinance every 1 min into paritioned parquet files
def poll_1m():
    # required to run this locally
    session = requests.Session(impersonate="chrome")
    # indices to poll for
    indices = '^DJI ^IXIC ^FTSE ^N225 000001.SS'
    # establish folder to store polled data
    save_path = "./market_data"
    os.makedirs(save_path, exist_ok=True)

    # yfinance ticker object to retrieve data
    tickers = yf.Tickers(indices, session=session)
    
    # loop to go through the market data for each index
    all_data = []
    for name, data in tickers.tickers.items():
        try:
            safe_name = name.replace("^", "") # remove ^ to store as file/folder name
            now = datetime.now() # store polled at time
            format_now = now.strftime("%Y-%m-%d %H:%M:%S") 
            # obtain market data per 1 min
            df = data.history(period="7d", interval="1m")
            df.reset_index(inplace=True)
            if df.empty:
                print(f"No data for {name} at {format_now}")
                continue 
            # get the data for the current min polled
            last_row = df.tail(1)
            # store the index name and the datetime polled
            last_row.insert(0, 'Index', safe_name)
            last_row.insert(1, 'StreamDatetime', now)
            
            # add to all data
            all_data.append(last_row)
        
        except Exception as e:
            print(f"Error fetching {name} at {format_now}: {e}")

    # output as parquet if data is added for any of the indices
    if all_data:
        # combine list dfs into one df
        combined_df = pd.concat(all_data, ignore_index=True)
        # converted to string due to the incompatability of the datatime retrieved from yfinance
        combined_df['Datetime'] = combined_df['Datetime'].astype(str)
        # ensure the polled datetime is a datetime format as this is needed for analysis
        combined_df["StreamDatetime"] = pd.to_datetime(combined_df["StreamDatetime"], utc=True)
        
        # create a folder for each index
        for index_value in combined_df["Index"].unique():
            index_dir = os.path.join(save_path, index_value)
            os.makedirs(index_dir, exist_ok=True)
            
            # save the index df as a parquet file with the timestamp in the filename
            filename = f"{now.strftime('%Y%m%d_%H%M%S')}.parquet"
            full_path = os.path.join(index_dir, filename)
            
            # append new rows to existing dataset, partitioned by index name
            combined_df[combined_df["Index"] == index_value].to_parquet(full_path, index=False)
            print(f"Saved {index_value} data at {format_now} to {full_path}")

        print(f"Saved all indices with partitioning at {format_now}")

# this is a function to simulate streaming by running the polling function every min
def fetch_data_periodically():
    while True:
        poll_1m()
        time.sleep(60)

if __name__ == "__main__":
    # stops running when the keyboard is interrupted 
    try:
        fetch_data_periodically()

    except KeyboardInterrupt:
        print("Streaming stopped")