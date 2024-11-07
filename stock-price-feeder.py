#!/usr/bin/env python3
# -*- coding: utf-8 -*-

# !pip install pandas

# Historical Daily Prices
# !pip install finance-datareader

import pandas as pd
import time, datetime, sys
import os, pathlib

# Ref: https://github.com/FinanceData/FinanceDataReader#quick-start
# import FinanceDataReader as fdr
# updated to import twelvedata instead of FinanceDataReader
import twelvedata

from twelvedata import TDClient


######### NEED TO UPDATE TO GET API_KEY FROM SOMEWHERE SECURE ##############
api_key = sys.argv[1]
td = TDClient(apikey=api_key)

# request the data from twelvedata and store it in a dictionary with a key for each stock
symbols = ['AAPL', 'MSFT']
stream_data = {}
for sym in symbols:
    ts = td.time_series(symbol=ticker, 
                        interval="4h", 
                        start_date= "2020-01-01").as_pandas()
    stream_data[sym] = ts

sys.stdout.reconfigure(encoding='utf-8')
sys.path.insert(0, str(pathlib.Path(__file__).parent.parent))
# print (sys.path, file=sys.stderr)

# convert dictionary to a DataFrame and align the dates
tech_df = pd.concat(stream_data, axis=1)
tech_df.columns = [sym for sym in symbols]

dates = tech_df.index.rename('Date')
init_date = list(dates)[0]
last_hist_date = list(dates)[-1]


init_delay_seconds = 30
interval = 5

scaler = tech_df['AAPL'][init_date]/tech_df['MSFT'][init_date]
aapl  = tech_df['AAPL']
msft  = tech_df['MSFT']
tech_df['scaledMSFT'] = msft*scaler
# print (tech_df[0:3])

print ('Sending daily AAPL and MSFT prices from %10s to %10s ...' % (str(init_date)[:10], str(last_hist_date)[:10]), flush=True, file=sys.stderr)
print ("... each day's data sent every %d hours ..." % (interval), flush=True, file=sys.stderr)
print ("... MSFT prices adjusted to match AAPL prices on %10s ..."  % (init_date), flush=True, file=sys.stderr)

from tqdm import tqdm
for left in tqdm(range(init_delay_seconds)):
    time.sleep(0.5)

for date in list(dates):       
    print ('%10s\t%.4f\t%.4f' % (str(date)[:10], tech_df['AAPL'][date], tech_df['scaledMSFT'][date]), flush=True)
    time.sleep(float(interval))

exit(0)

# Real Time Prices
# Eventually we want to make it a day trading platform in the spirit of 
# https://www.investopedia.com/articles/trading/05/011705.asp
# !pip install yahoo_fin
import datetime, time
from yahoo_fin import stock_info

for t in range(10):
    now = datetime.datetime.now()
    aappl_price = stock_info.get_live_price('AAPL')
    msft_price = stock_info.get_live_price('MSFT')
    print ('%10s\t%.4f\t%.4f' % (str(now)[:19], aapl_price, msft_price))
    time.sleep(5.0)

exit(0)
