import pandas as pd
import numpy as np
import mysql.connector

from os import listdir, remove
from os.path import isfile, join

default_schema = 'stocks'
default_date_field = 'Date'

def get_connection():
    conn = mysql.connector.connect(
        host='localhost',
        user='epdrumond',
        password='epdf1991',
        database='stocks',
        auth_plugin='mysql_native_password'
    )

    return conn

#Read raw data from csv files
def read_raw_data(folder_path):
    files = [file for file in listdir(folder_path) if isfile(join(folder_path, file))]
    files = {file: file.split('.')[0] for file in files}

    raw_data = []
    for file, ticker in files.items():
        raw_ticker_data = pd.read_csv(join(folder_path, file))
        raw_ticker_data.insert(
            1,
            'Ticker',
            [ticker for i in range(len(raw_ticker_data))]
        )
        raw_data.append(raw_ticker_data)

    return pd.concat(raw_data)

#Apply transformations in raw data for insert on DB table
def transform_input_data(raw_data):
    date_fields = [default_date_field, 'ExtractionDate']
    for date in date_fields:
        raw_data[date] = ["'" + str(x)[:19] + "'" for x in raw_data[date]]

    decimal_fields = ['Open', 'High', 'Low', 'Close', 'Adj Close']
    for col in decimal_fields:
        raw_data[col] = raw_data[col].round(6)

    raw_data['Ticker'] = ["'" + ticker + "'" for ticker in raw_data['Ticker']]

    return raw_data

#Remove data already inserted into the DB table from raw data
def check_new_data(table_name, raw_data, cursor):
    query = 'select Ticker, max({}) as max_date from {}.{} group by 1;'.format(
        default_date_field, default_schema, table_name
    )

    #Get latest date from table
    try:
        cursor.execute(query)
        max_date = pd.DataFrame(cursor.fetchall(), columns = ['Ticker', 'max_date'])
    except:
        return raw_data

    #Filter new data to avoid duplicity
    new_data = raw_data.join(
        on = 'Ticker',
        other = max_date.set_index('Ticker')
    )

    new_data['flag_new'] = [
        1 if (max_date != max_date or np.datetime64(date) > max_date) else 0
        for date, max_date
        in new_data[['Date', 'max_date']].values
    ]

    new_data = new_data[new_data['flag_new'] == 1]
    new_data = new_data.drop(columns=['max_date', 'flag_new']).sort_values(by=default_date_field)

    return new_data

#Bundle every line of data to be inserted into a single command
def create_insert_command(table_name, data):
    values = ','.join([
        '(' + ','.join([str(val) for val in row]) + ')'
        for row in data.values
    ])
    return f'insert into {default_schema}.{table_name} values {values};'

#Main funcion: Load new data into DB table
def load_table(raw_data_folder, table_name):
    #Get database connection
    conn = get_connection()
    cursor = conn.cursor(buffered=True)

    #Append new values to table
    raw_data = read_raw_data(raw_data_folder)

    input_data = check_new_data(
        table_name=table_name,
        raw_data=raw_data,
        cursor=cursor
    )

    if len(input_data) > 0:
        cursor.execute(create_insert_command(
            table_name=table_name,
            data=transform_input_data(input_data)
        ))
        conn.commit()

    conn.close()

if __name__ == '__main__':
    raw_data_folder = '/home/edilson/Desktop/Edilson/Python/stock_data_prediction_pipeline/raw_data'

    load_table(
        raw_data_folder=raw_data_folder,
        table_name = 'daily_stock_prices'
    )
