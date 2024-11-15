#!/usr/bin/env python3
# -*- coding: utf-8 -*-

#!pip install pandas

# Historical Daily Prices

import pandas as pd
import time, datetime, sys
import os, pathlib
import nltk

import twelvedata

from pyspark.sql import SparkSession
from twelvedata import TDClient
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from datetime import datetime, timedelta


# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockDataAnalysis") \
    .getOrCreate()


######### NEED TO UPDATE TO GET API_KEY FROM SOMEWHERE SECURE ##############
import os
api_key = os.getenv("TWELVE_DATA_API_KEY")
if not api_key:
    print("API key is missing!")
    sys.exit(1)
    
td = TDClient(apikey=api_key)

# request the data from twelvedata and store it in a dictionary with a key for each stock
symbols = ['AAPL', 'MSFT']
stream_data = {}
for sym in symbols:
    print(f"(starting data collection for {sym}")
    symbol_data = []  # list to store the data for the current symbol
    set_start_date = "2023-01-03"
    # Loop through the 40 days, requesting one day of data at a time
    for i in range(40):
        try:
            ts = td.time_series(symbol=sym, 
                                interval="1day", 
                                start_date=set_start_date,
                                outputsize=1).as_pandas()
            print(ts)
            symbol_data.append(ts[['close']])  # append just the 'close' column data
        except Exception as e:
            print(f"Error fetching data: {e}")
            time.sleep(10)
            continue
        
        # wait 15 seconds before the next request to avoid hitting api credit limit
        print('Next iteration in 10 seconds')
        time.sleep(10)

        # Update the start_date to the next day after the last request
        set_start_date = pd.to_datetime(set_start_date) + pd.Timedelta(days=1)
        set_start_date = set_start_date.strftime("%Y-%m-%d")
        print(set_start_date)
    """
    # after collecting the 40 days of data, concatenate the list into a single DataFrame
    full_data = pd.concat(symbol_data)
    
    # rename the 'close' column for the symbol and store it in the stream_data dictionary
    stream_data[sym] = full_data.rename(columns={'close': f"{sym}_price"})

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
# print (sys.path, file=sys.stderr)

print(f"AAPL Data:\n{stream_data['AAPL'].head()}")
print(f"MSFT Data:\n{stream_data['MSFT'].head()}")

# convert dictionary to a pandas dataframe and then a spark dataframe
tech_df = pd.concat(stream_data.values(), axis=1)
tech_df['Date'] = tech_df.index
tech_df = tech_df[['Date', 'AAPL_price', 'MSFT_price']]
spark_df = spark.createDataFrame(tech_df)


# align the dates for the two stocks


# make a dataframe with the correct dates
start_date = "2023-01-03"
latest_date = spark_df.agg(F.max("Date")).collect()[0][0]
latest_date = str(latest_date).split(' ')[0]  # Get only the date part

date_range = spark.range(0, (datetime.strptime(str(latest_date), '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1) \
    .withColumn("Date", F.expr(f"date_add('{start_date}', cast(id as int))")) \
    .select("Date")

aapl_df = spark_df.select("Date", "AAPL_price").join(date_range, on="Date", how="right")  # join the stock data with the correct dates
msft_df = spark_df.select("Date", "MSFT_price").join(date_range, on="Date", how="right")

# joing the two dataframes on the date column
aligned_df = aapl_df.join(msft_df, on="Date", how="outer").orderBy("Date")


# forward fill nulls
window_spec = Window.orderBy("Date").rowsBetween(-1, 0)
aligned_df = aligned_df \
    .withColumn("AAPL_price", F.last("AAPL_price", ignorenulls=True).over(window_spec)) \
    .withColumn("MSFT_price", F.last("MSFT_price", ignorenulls=True).over(window_spec))
print(aligned_df.head(10))

# calculate the values for the moving averages and add a column to track them

# Define the window spec to calculate the moving averages
window_spec_40 = Window.orderBy("Date").rowsBetween(-40, 0)  # 40-day window (last 40 rows)
window_spec_10 = Window.orderBy("Date").rowsBetween(-10, 0)  # 10-day window (last 10 rows)

aligned_df = aligned_df \
    .withColumn("aapl10Day", F.avg("AAPL_price").over(window_spec_10)) \
    .withColumn("aapl40Day", F.avg("AAPL_price").over(window_spec_40)) \
    .withColumn("msft10Day", F.avg("MSFT_price").over(window_spec_10)) \
    .withColumn("msft40Day", F.avg("MSFT_price").over(window_spec_40))

# check the most recent relationship between the 10 and 40 day averages
latest_averages = aligned_df.orderBy(F.desc("Date")).select("aapl10Day", "aapl40Day", "msft10Day", "msft40Day").first()

print(f"(latest_averages:{latest_averages})")

aapl_curr = "higher" if latest_averages["aapl10Day"] > latest_averages["aapl40Day"] else "lower"
msft_curr = "higher" if latest_averages["msft10Day"] > latest_averages["msft40Day"] else "lower"


exit(0)"""
