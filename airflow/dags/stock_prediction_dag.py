from airflow.models import DAG
from airflow.hooks.mysql_hook import MySqlHook
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago

from datetime import datetime, timedelta

from modules.functions import extract_stock_prices, load_database, predict_stocks

mysql = MySqlHook(mysql_conn_id='mysql_default')

ARGS = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime.now() - timedelta(days=1)
}

stocks = [
    'WMT', 'AMZN', 'AAPL', 'CVS', 'UNH',
    'BRK-B', 'MCK', 'ABC', 'GOOG', 'XOM'
]

DEST = '/home/edilson/Desktop/Edilson/Python/stock_data_prediction_pipeline/raw_data'

dag = DAG(
    dag_id='stock_prediction_dag',
    default_args=ARGS,
    schedule_interval='34 */1 * * *',
    max_active_runs=1
)

extract_stock_data = PythonOperator(
    task_id='get_stock_prices',
    python_callable=extract_stock_prices.query_stocks,
    provide_context=False,
    op_kwargs={
        'ticker_list': stocks,
        'dest': DEST,
        'conn': mysql.get_conn()
    },
    dag=dag
)

load_data_into_db = PythonOperator(
    task_id='load_data_into_db',
    python_callable=load_database.load_table,
    provide_context=False,
    op_kwargs={
        'raw_data_folder': DEST,
        'table_name': 'daily_stock_prices',
        'conn': mysql.get_conn()
    },
    dag = dag
)

pred_stock = PythonOperator(
    task_id='pred_stock',
    python_callable=predict_stocks.make_prediction,
    provide_context=False,
    op_kwargs={
        'conn': mysql.get_conn(),
        'ticker_list': stocks
    },
    dag=dag
)

extract_stock_data >> load_data_into_db
load_data_into_db >> pred_stock
