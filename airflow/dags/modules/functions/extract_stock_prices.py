import pandas as pd
import numpy as np
import yfinance as yf
from os.path import join, dirname, realpath
from datetime import datetime

def check_last_date(ticker, conn):
    query = f"select max(Date) from stocks.daily_stock_prices where Ticker = '{ticker}'"
    return pd.read_sql(query, conn).values[0][0]

def save_raw_data(ticker, dest, conn):
    last_date = check_last_date(ticker, conn)
    period = '{}d'.format(
        int((np.datetime64('today') - last_date) / np.timedelta64(1, 'D'))
    )
    data = yf.download(tickers=ticker, period=period, interval='1d')
    data.index.name = 'Date'
    data['ExtractionDate'] = datetime.today()

    file_name = ticker + '.csv'
    data.to_csv(join(dest, file_name), index = True)

def query_stocks(ticker_list, dest, conn, **kwargs):
    for ticker in ticker_list:
        save_raw_data(
            ticker=ticker,
            dest=dest,
            conn=conn
        )
