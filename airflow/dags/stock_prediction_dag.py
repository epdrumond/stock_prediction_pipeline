from airflow.models import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime

from modules.functions import extract_stock_prices, load_database

ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2021,1,1)
}

stocks = [
    'WMT', 'AMZN', 'AAPL', 'CVS', 'UNH',
    'BRK-B', 'MCK', 'ABC', 'GOOG', 'XOM'
]

extract_params = {
    'period': '3d',
    'interval': '1d',
    'dest': '/home/edilson/Desktop/Edilson/Python/stock_data_prediction_pipeline/raw_data'
}

dag = DAG(
    dag_id='stock_prediction_dag',
    default_args=ARGS,
    schedule_interval='* * * * *',
    max_active_runs=1
)

extract_stock_data = PythonOperator(
    task_id='get_stock_prices',
    python_callable=extract_stock_prices.query_stocks,
    provide_context=False,
    op_kwargs={
        'ticker_list': stocks,
        'period': extract_params['period'],
        'interval': extract_params['interval'],
        'dest': extract_params['dest'],
    },
    dag=dag
)

load_data_into_db = PythonOperator(
    task_id='load_data_into_db',
    python_callable=
)

extract_stock_data
