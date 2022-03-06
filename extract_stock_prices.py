import pandas as pd
import yfinance as yf
from os.path import join

def query_stock_prices(ticker, period, interval, dest):
    data = yf.download(tickers = ticker, period = period, interval = interval)
    return data

if __name__ == '__main__':
    print(query_stock_prices(
        ticker = 'UBER',
        period = '90d',
        interval = '1d'
    ))
