from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup

class financial_data_fetcher:
    def __init__(self, symbol):
        self.symbol = symbol
        self.download_path = "/opt/airflow/data"

    def financial_data_path(self):
        import yfinance as yf
        import os
        import shutil
        import pandas as pd

        df_stock = yf.download(self.symbol, period = '5y', interval="1wk")
        df_stock = df_stock.reset_index()

        os.makedirs(self.download_path, exist_ok=True)  

        csv_path = os.path.join(self.download_path, f'{self.symbol}_market_data.csv')

        df_stock.to_csv(csv_path, index = False)

        return csv_path

        # #download_path = "C:\\Users\\sammy\\Documents\\AirflowNotebook\\AirflowNotebook"

        # #filepath = os.path.join(download_path, [file for file in os.listdir(download_path) if file.endswith('.csv')][0])

        # kwargs['ti'].xcom_push(key='AAPL_finance_data', value = csv_path)

def AAPL_finance_data(**kwargs):
    financial_data_fetcherAAPL = financial_data_fetcher(symbol='AAPL')
    csv_path = financial_data_fetcherAAPL.financial_data_path()
    kwargs['ti'].xcom_push(key='AAPL_csv_path', value=csv_path)

def BTC_USD_finance_data(**kwargs):
    financial_data_fetcherBTC_USD = financial_data_fetcher(symbol='BTC-USD')
    csv_path = financial_data_fetcherBTC_USD.financial_data_path()
    kwargs['ti'].xcom_push(key='BTC_USD_csv_path', value=csv_path)
    
def fetch_financial_data():

    with TaskGroup("fetch_financial_data", tooltip="Fetching data for different stocks") as group:

        AAPL_data = PythonOperator(
            task_id='AAPL_finance_data',
            python_callable=AAPL_finance_data,
            provide_context=True,
        )

        BTC_USD_data = PythonOperator(
            task_id='BTC_USD_finance_data',
            python_callable=BTC_USD_finance_data,
            provide_context=True,
        )
        
        return group