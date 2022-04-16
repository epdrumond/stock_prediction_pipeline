import pandas as pd
import numpy as np
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

    pred_data = stock_data.iloc[-1,:].drop(index=['Open']).values.reshape(1,-1)
    stock_data = stock_data.dropna()

    model = get_linear_regression(stock_data)
    pred = model.predict(pred_data)

    return pred


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
