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
    symbol_data = []  # list to store the data for the current symbol
    
    # Loop through the 40 days, requesting one day of data at a time
    for i in range(40):
        ts = td.time_series(symbol=sym, 
                            interval="1day", 
                            start_date=start_date,
                            outputsize=1).as_pandas()
        symbol_data.append(ts[['close']])  # append just the 'close' column data
        
        # wait 15 seconds before the next request to avoid hitting api credit limit
        time.sleep(15)

        # Update the start_date to the next day after the last request
        start_date = pd.to_datetime(start_date) + pd.Timedelta(days=1)
        start_date = start_date.strftime("%Y-%m-%d")
    
    # after collecting the 40 days of data, concatenate the list into a single DataFrame
    full_data = pd.concat(symbol_data)
    
    # rename the 'close' column for the symbol and store it in the stream_data dictionary
    stream_data[sym] = full_data.rename(columns={'close': f"{sym}_price"})

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
# print (sys.path, file=sys.stderr)

# convert dictionary to a pandas dataframe and then a spark dataframe
tech_df = pd.concat(stream_data.values(), axis=1)
tech_df['Date'] = tech_df.index
tech_df = tech_df[['Date', 'AAPL_price', 'MSFT_price']]
spark_df = spark.createDataFrame(tech_df)


# align the dates for the two stocks


# make a dataframe with the correct dates
start_date = "2024-01-01"
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


aapl_curr = "higher" if latest_averages["aapl10Day"] > latest_averages["aapl40Day"] else "lower"
msft_curr = "higher" if latest_averages["msft10Day"] > latest_averages["msft40Day"] else "lower"


# Real Time Prices
# Eventually we want to make it a day trading platform in the spirit of 
# https://www.investopedia.com/articles/trading/05/011705.asp
# !pip install yahoo_fin
# this version isn't actually real-time prices because we wouldn't get enough examples of when 


# aaplPrice and msftPrice streams
for t in range(100):
    # request the next day of data for AAPL and MSFT (starting with the first day after the most recent date in aligned_df)
    latest_date = aligned_df.agg(F.max("Date")).collect()[0][0]
    latest_date = str(latest_date).split(' ')[0]  # Get only the date part
    next_date = (datetime.strptime(str(latest_date), "%Y-%m-%d") + timedelta(days=1)).strftime("%Y-%m-%d")
    #added a try statement due to some errors in previous runs
    try: #added a try statement due to some errors in previous runs
        new_aapl_price = td.time_series(symbol="AAPL", interval="1day", start_date=next_date, end_date=next_date).as_pandas().iloc[0]['close']
        new_msft_price = td.time_series(symbol="MSFT", interval="1day", start_date=next_date, end_date=next_date).as_pandas().iloc[0]['close']
    except Exception as e:
        print(f"Error fetching data: {e}")
        continue
    
    # append the date and prices as a new row in aligned_df
    new_row = spark.createDataFrame([(next_date, new_aapl_price, new_msft_price)], ["Date", "AAPL_price", "MSFT_price"])
    aligned_df = aligned_df.union(new_row)

    
    # calculate the updated 40 and 10 day averages for each and store them in the appropriate columns
    aligned_df = aligned_df \
        .withColumn("aapl10Day", F.avg("AAPL_price").over(window_spec_10)) \
        .withColumn("aapl40Day", F.avg("AAPL_price").over(window_spec_40)) \
        .withColumn("msft10Day", F.avg("MSFT_price").over(window_spec_10)) \
        .withColumn("msft40Day", F.avg("MSFT_price").over(window_spec_40))

    
    # update the values of latest_aapl10Day, latest_aapl40Day, latest_msft10Day, and latest_msft40Day
    latest_averages = aligned_df.orderBy(F.desc("Date")).select("aapl10Day", "aapl40Day", "msft10Day", "msft40Day").first()
    
    # make the trading recommendations if appropriate
    
    if aapl_curr == "higher" and latest_averages["aapl10Day"] < latest_averages["aapl40Day"]:
        print(f"{latest_averages['Date'].strftime('%Y-%m-%d')} sell aapl")
        aapl_curr = "lower"
    elif aapl_curr == "lower" and latest_averages["aapl10Day"] > latest_averages["aapl40Day"]:
        print(f"{latest_averages['Date'].strftime('%Y-%m-%d')} buy aapl")

        aapl_curr = "higher"

    if msft_curr == "higher" and latest_averages["msft10Day"] < latest_averages["msft40Day"]:
        print(f"{latest_averages['Date'].strftime('%Y-%m-%d')} sell msft")

        msft_curr = "lower"
    elif msft_curr == "lower" and latest_averages["msft10Day"] > latest_averages["msft40Day"]:
        print(f"{latest_averages['Date'].strftime('%Y-%m-%d')} buy msft")

        msft_curr = "higher"
        
    time.sleep(25.0)

exit(0)
