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

if __name__ == '__main__':
    current_folder = dirname(realpath('__file__'))

    save_raw_data(
        ticker = 'UBER',
        period = '2d',
        interval = '1d',
        dest = join(current_folder, 'raw_data')
    )
