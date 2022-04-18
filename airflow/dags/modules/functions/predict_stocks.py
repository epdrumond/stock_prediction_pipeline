import pandas as pd
import numpy as np
from datetime import datetime

from statsmodels.tsa.stattools import acf
from sklearn.linear_model import LinearRegression

import mysql.connector

import warnings
warnings.filterwarnings('ignore')

#Identify "Open" lags to include in the prediction model -----------------------
def get_lags(data):
    corr, interval = acf(
        x=data['Open'].values,
        nlags=200,
        alpha=0.05
    )

    max_lag = 0
    test = []
    for coef, [min_interval, max_interval] in zip(corr[1:], interval[1:]):
        min_interval -= coef
        max_interval -= coef

        if coef < min_interval or coef > max_interval:
            max_lag += 1
        else:
            return max_lag

    return max_lag

#Create the dataset for the prediction -----------------------------------------
def get_dataset(data, volume_lags=10):
    stock_data = data.copy()
    cols_to_keep = ['Date', 'Open', 'Volume']
    stock_data = stock_data[cols_to_keep]

    nlags = get_lags(stock_data)

    next_date_data = pd.DataFrame()
    next_date_data['Date'] = [stock_data['Date'].max() + np.timedelta64(1, 'D')]
    stock_data = pd.concat([stock_data, next_date_data], axis=0)

    for lag in range(1, nlags+1):
        col_name = 'Open_lag_' + str(lag)
        stock_data[col_name] = stock_data['Open'].shift(lag)

    for lag in range(1,11):
        col_name = 'Volume_lag_' + str(lag)
        stock_data[col_name] = stock_data['Volume'].shift(lag)

    stock_data.set_index('Date', inplace = True)
    stock_data.drop(columns = 'Volume', inplace = True)
    stock_data = stock_data[stock_data.iloc[:,-1].isnull() == False]

    return stock_data

#Create the Linear Regression model --------------------------------------------
def get_linear_regression(data, target_field='Open'):
    x_data = data.drop(columns=target_field).values
    y_data = data[target_field].values

    regressor = LinearRegression(normalize=True)
    regressor.fit(x_data, y_data)

    return regressor

#Predict the next day for the stock data ---------------------------------------
def predict_stock(data):
    stock_data = get_dataset(data)

    pred_date = stock_data.iloc[-1,:].name
    pred_data = stock_data.iloc[-1,:].drop(index=['Open']).values.reshape(1,-1)

    stock_data = stock_data.dropna()

    model = get_linear_regression(stock_data)
    pred = model.predict(pred_data)[0]

    return {'Date': pred_date, 'Value': pred}

#Store predicted data into table -----------------------------------------------
def store_prediction(conn, data):
    insert_command = f'''
    insert into stocks.predicted_stock_prices values (
        '{data['Date']}',
        '{data['Ticker']}',
        {data['Value']},
        '{datetime.now().date()} 00:00:00'
    )'''

    check_date_command = f'''
    select exists(
        select *
        from stocks.daily_stock_prices
        where
            Date = '{data['Date']}' and
            Ticker = '{data['Ticker']}'
        )
    '''
    date_flag = pd.read_sql(check_date_command, conn).values[0][0]

    if not date_flag:
        cursor = conn.cursor()
        cursor.execute(insert_command)
        conn.commit()

def make_prediction(conn, ticker_list):
    for ticker in ticker_list:
        query = f'''
            select
                Date,
                Open,
                Volume
            from stocks.daily_stock_prices where Ticker = '{ticker}'
        '''
        data = pd.read_sql(query, conn)

        pred_data = predict_stock(data)
        pred_data.update({'Ticker': ticker})

        store_prediction(conn, pred_data)

if __name__ == '__main__':
    conn = mysql.connector.connect(
        user='epdrumond',
        password='epdf1991',
        host='localhost',
        database='stocks'
    )

    query = "select Date, Open, Volume from stocks.daily_stock_prices where Ticker = 'AMZN'"
    test_data = pd.read_sql(query, conn)

    print(predict_stock(test_data))
