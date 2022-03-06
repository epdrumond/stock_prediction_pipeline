import pandas as pd
import mysql.connector
from sqlalchemy import create_engine

default_schema = 'stocks'
default_date_field = 'Date'

def get_connection(mode = 'append'):
    if mode == 'create':
        conn = create_engine('mysql+mysqlconnector://epdrumond:epdf1991@localhost/stocks')
    elif mode == 'append':
        conn = mysql.connector.connect(
            host = 'localhost',
            user = 'epdrumond',
            password = 'epdf1991',
            database = 'curso_alura',
            auth_plugin = 'mysql_native_password'
        )

    return conn

def check_new_data(table_name, data, cursor):
    #Get latest date from table
    max_date = cursor.execute(
        f'select max({date_field}) from {default_schema}.{table_name};'
    ).fetchall()[0][0]

    #Filter new data to avoid duplicity
    new_data = data[data[default_date_field] > max_date]
    return new_data

def create_insert_command(table_name, data):
    values = ','.join([
        '(' + ','.join([str(val) for val in row]) + ')'
        for row in data.reset_index()
    ])
    return f'insert into {default_schema}.{table_name} values {values};'

def load_table(table_name, data):
    #Get database connection
    conn = get_connection()
    cursor = conn.cursor()

    #Check whether table already exists
    cursor.execute(f'use {default_schema};')
    cursor.execute('show tables;')
    tables = cursor.fetchall()[0]
    mode = 'append' if table_name in tables else 'create'

    if mode == 'create':
        #Create new table in the db
        conn = get_connection(mode)
        data.to_sql(
            name = table_name,
            con = conn,
            schema = default_schema,
            index_label = default_date_field
        )
    elif mode == 'append':
        #Append new values to table
        input_data = check_new_data(
            table_name = table_name,
            data = data,
            cursor = cursor
        )
        cursor.execute(
            create_insert_command(
                table_name = table_name,
                data = input_data
        ))

        conn.commit()
        
    conn.close()

if __name__ == '__main__':
    data = pd.read_csv(
        '/home/edilson/Desktop/Edilson/Python/'
        'stock_data_prediction_pipeline/raw_data/UBER.csv')

    load_table(
        table_name = 'UBER',
        data = data
    )
