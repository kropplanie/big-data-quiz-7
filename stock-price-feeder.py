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
import pandas as pd
from twelvedata import TDClient
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Initialize Spark session
spark = SparkSession.builder \
    .appName("StockDataAnalysis") \
    .getOrCreate()


######### NEED TO UPDATE TO GET API_KEY FROM SOMEWHERE SECURE ##############
api_key = sys.argv[1]
td = TDClient(apikey=api_key)

# request the data from twelvedata and store it in a dictionary with a key for each stock
symbols = ['AAPL', 'MSFT']
stream_data = {}
for sym in symbols:
    ts = td.time_series(symbol=ticker, 
                        interval="1day", 
                        start_date= "2024-01-01",
                        outputsize=40).as_pandas()
    stream_data[sym] = ts

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
# print (sys.path, file=sys.stderr)

# convert dictionary to a pandas dataframe and then a spark dataframe
tech_df = pd.concat(stream_data, axis=1)
tech_df.columns = [f"{sym}_price" for sym in symbols]
tech_df['Date'] = tech_df.index
tech_df = tech_df[['Date', 'AAPL_price', 'MSFT_price']]
spark_df = spark.createDataFrame(tech_df)


# align the dates for the two stocks

# make a dataframe with the correct dates
start_date = "2024-01-01"
latest_date = spark_df.agg(F.max("Date")).collect()[0][0]date_range = spark.range(0, (datetime.strptime(end_date, '%Y-%m-%d') - datetime.strptime(start_date, '%Y-%m-%d')).days + 1) \
    .withColumn("Date", F.expr(f"date_add('{start_date}', id)")) \
    .select("Date")
date_range = spark.range(0, (latest_date - datetime.strptime(start_date, '%Y-%m-%d')).days + 1) \
    .withColumn("Date", F.expr(f"date_add('{start_date}', id)")) \
    .select("Date")

aapl_df = spark_df.select("Date", "AAPL_price").join(date_range, on="Date", how="right") # join the stock data with the correct dates
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
window_spec_40 = Window.orderBy("Date").rowsBetween(-40, 0)  # 40-day window
window_spec_10 = Window.orderBy("Date").rowsBetween(-10, 0)  # 10-day window

# add the 40-day and 10-day moving averages for each stock
aligned_df = spark_df.withColumn("aapl40Day", F.avg("AAPL_price").over(window_spec_40))
aligned_df = spark_df.withColumn("msft40Day", F.avg("MSFT_price").over(window_spec_40))

aligned_df = spark_df.withColumn("aapl10Day", F.avg("AAPL_price").over(window_spec_10))
aligned_df = spark_df.withColumn("msft10Day", F.avg("MSFT_price").over(window_spec_10))



# get the most recent values of the averages
latest_averages = aligned_df.orderBy(F.desc("Date")).select(
    "aapl10Day", "aapl40Day", "msft10Day", "msft40Day"
).first()

latest_aapl10Day = latest_averages["aapl10Day"]
latest_aapl40Day = latest_averages["aapl40Day"]
latest_msft10Day = latest_averages["msft10Day"]
latest_msft40Day = latest_averages["msft40Day"]

# set tracker variables based on the comparison of the 10 day and 40 day averages
aapl_curr = "higher" if aapl10Day > aapl40Day else "lower"
msft_curr = "higher" if msft10Day > msft40Day else "lower"


# Real Time Prices
# Eventually we want to make it a day trading platform in the spirit of 
# https://www.investopedia.com/articles/trading/05/011705.asp
# !pip install yahoo_fin
# this version isn't actually real-time prices because we wouldn't get enough examples of when 


# aaplPrice and msftPrice streams
for t in range(100):
    # request the next day of data for AAPL and MSFT
    
    
    # append the date and prices as a new row in aligned_df

    # calculate the updated 40 and 10 day averages for each and store them in the appropriate columns

    # update the values of latest_aapl10Day, latest_aapl40Day, latest_msft10Day, and latest_msft40Day

    # if aapl_curr = "higher" and latest_aapl10Day < latest_aapl40Day
        # print "<<date>> "sell aapl"
        # set aapl_curr to "lower"
    # elif aapl_curr = "lower" and latest_aapl10Day > latest_aapl40Day
        # print "<<date>> "buy aapl"
        # set aapl_curr to "higher"

    # if msft_curr = "higher" and latest_msft10Day < latest_msft40Day
        # print "<<date>> "sell msft"
        # set msft_curr to "lower"
    # elif msft_curr = "lower" and latest_msft10Day > latest_msft40Day
        # print "<<date>> "buy msft"
        # set msft_curr to "higher"
    
    time.sleep(15.0)

exit(0)
