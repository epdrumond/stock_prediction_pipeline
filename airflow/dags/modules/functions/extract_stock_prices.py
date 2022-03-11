import pandas as pd
import yfinance as yf
from os.path import join, dirname, realpath
from datetime import datetime

def save_raw_data(ticker, period, interval, dest):
    data = yf.download(tickers = ticker, period = period, interval = interval)
    data.index.name = 'Date'
    data['ExtractionDate'] = datetime.today()

    file_name = ticker + '.csv'
    data.to_csv(join(dest, file_name), index = True)

def query_stocks(ticker_list, period, interval, dest, **kwargs):
    for ticker in ticker_list:
        save_raw_data(
            ticker=ticker,
            period=period,
            interval=interval,
            dest=dest
        )

if __name__ == '__main__':
    current_folder = dirname(realpath('__file__'))
    stocks = [
        'WMT', 'AMZN', 'AAPL', 'CVS', 'UNH', 'BRK-B', 'MCK',
        'ABC', 'GOOG', 'XOM']

    for stock in stocks:
        save_raw_data(
            ticker = stock,
            period = '100d',
            interval = '1d',
            dest = join(current_folder, 'raw_data')
        )
