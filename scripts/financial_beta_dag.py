#import yfinance as yf
from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from groups.group_financial_data import fetch_financial_data
from datetime import datetime

# def _fetch_financial_data_AAPL(**kwargs):
#     import yfinance as yf
#     import os
#     import shutil
#     import pandas as pd

#     AAPLindex_symbol = "AAPL"

#     df_stock = yf.download(AAPLindex_symbol, period = '5y', interval="1wk")
#     df_stock = df_stock.reset_index()

#     download_path = "C:"

#     if os.path.exists(download_path):
#         shutil.rmtree(download_path)

#     os.makedirs(download_path, exist_ok=True)  

#     csv_path = os.path.join(download_path, 'AAPL_data.csv')

#     df_stock.to_csv(csv_path, index = False)

#     #download_path = "C:"

#     #filepath = os.path.join(download_path, [file for file in os.listdir(download_path) if file.endswith('.csv')][0])

#     kwargs['ti'].xcom_push(key='AAPL_finance_data', value = csv_path)

def wipe_old_csv_ifexists(**kwargs):
    import os
    import glob

    download_path = "/opt/airflow/data"
    csv_files = glob.glob(os.path.join(download_path, '*.csv'))

    for csv_file in csv_files:
        os.remove(csv_file)

    return f"Removed {len(csv_files)} CSV files from {download_path}"


def _fetch_market_data(**kwargs):
    import yfinance as yf
    import os
    import shutil
    import pandas as pd

    market_symbol = '^GSPC'

    df_market = yf.download(market_symbol, period = '5y', interval="1wk")
    df_market = df_market.reset_index()

    download_path = "/opt/airflow/data"

    # if os.path.exists(download_path):
    #     shutil.rmtree(download_path)

    os.makedirs(download_path, exist_ok=True)  

    csv_path = os.path.join(download_path, 'market_data.csv')

    df_market.to_csv(csv_path, index = False)

    #download_path = "C:\\Users\\sammy\\Documents\\AirflowNotebook\\AirflowNotebook"

    #filepath = os.path.join(download_path, [file for file in os.listdir(download_path) if file.endswith('.csv')][0])

    kwargs['ti'].xcom_push(key='market_finance_data', value = csv_path)


def _beta(**kwargs):
    import os
    import pandas as pd
    import numpy as np

    ti = kwargs['ti']

    AAPL_csv = kwargs['ti'].xcom_pull(key='AAPL_csv_path', task_ids = 'fetch_financial_data.AAPL_finance_data')
    BTC_USD_csv = kwargs['ti'].xcom_pull(key='BTC_USD_csv_path', task_ids='fetch_financial_data.BTC_USD_finance_data')
    
    market_csv = kwargs['ti'].xcom_pull(key='market_finance_data', task_ids = 'fetch_market_data')
    market_finance_data = pd.read_csv(market_csv, index_col = 'Date')

    beta_dictionary = {}

    print('AAPL CSV path is:', AAPL_csv) 
    print('BTC_USD CSV path is:', BTC_USD_csv)
    print('Market CSV path is:', market_csv)

    for csv_path in [AAPL_csv, BTC_USD_csv]:
        finance_data = pd.read_csv(csv_path, index_col = 'Date')

        df_merged = pd.merge(finance_data, market_finance_data, on='Date')

        df_merged['Stock_Returns'] = df_merged['Adj Close_x'].pct_change()
        df_merged['Market_Returns'] = df_merged['Adj Close_y'].pct_change()
        
        covariance = np.cov(df_merged['Stock_Returns'].dropna(), df_merged['Market_Returns'].dropna())[0][1]
        variance = np.var(df_merged['Market_Returns'].dropna())
        beta = covariance / variance
        beta_dictionary[csv_path[:-4]] = beta

    return beta_dictionary

def _plot(**kwargs):
    import datetime
    from datetime import date
    import pandas as pd
    import matplotlib
    import matplotlib.pyplot as plt
    import numpy as np
    import sklearn
    from sklearn import preprocessing

    AAPL_csv = kwargs['ti'].xcom_pull(key='AAPL_csv_path', task_ids = 'fetch_financial_data.AAPL_finance_data')
    BTC_USD_csv = kwargs['ti'].xcom_pull(key='BTC_USD_csv_path', task_ids='fetch_financial_data.BTC_USD_finance_data')
    market_csv = kwargs['ti'].xcom_pull(key='market_finance_data', task_ids = 'fetch_market_data')

    AAPL = pd.read_csv(
        AAPL_csv) #index_col='Date')
    BTC_USD = pd.read_csv(
        BTC_USD_csv)
        #index_col='Date')
    market = pd.read_csv(
        market_csv)
        #index_col='Date')

    # AAPL_Adj_Close_array = np.array(AAPL['Adj Close'])
    # print(preprocessing.normalize([AAPL_Adj_Close_array]))
    
    # ['Adj Close']

    def date_to_datetime(date):
        strp_date = datetime.datetime.strptime(date, '%Y-%m-%d').date()
        return strp_date

    for df in [AAPL, BTC_USD, market]:
        df['Date'] = df['Date'].apply(date_to_datetime)

    plt.plot(AAPL['Date'], preprocessing.normalize([AAPL['Adj Close']])[0], label="AAPL", linestyle="--")
    plt.plot(BTC_USD['Date'], preprocessing.normalize([BTC_USD['Adj Close']])[0], label="BTC-USD", linestyle="--")
    plt.plot(market['Date'], preprocessing.normalize([market['Adj Close']])[0], label="GSPC Market", linestyle="-")
    #fig, ax = plt.subplots()
    plt.ylabel('Normalised Adj Close')
    plt.xlabel('Time (years)')

    #plt.xticks(np.arange(market['Date'].iloc[0], market['Date'].iloc[-1], step=365))
    today = date.today()
    title = today.strftime("%d-%m-%Y")
    plt.title(f'Volatility of stocks compared to market average: {title}')
    plt.legend()
    plt.savefig(f'/opt/airflow/data/stocks{title}.png', bbox_inches='tight')
    # plt.show()

with DAG("financial_beta_dag", start_date=datetime(2024, 7, 11), 
    schedule_interval='@daily', catchup=False) as dag:
 
    wipe_old_csv_ifexists = PythonOperator(
        task_id='wipe_old_csv_ifexists',
        python_callable=wipe_old_csv_ifexists,
        provide_context=True,
    )

    fetch_financial_data = fetch_financial_data()

    fetch_market_data = PythonOperator(
        task_id='fetch_market_data',
        python_callable=_fetch_market_data,
        provide_context=True,
    )

    # branch = BranchPythonOperator(
    #     task_id = 'branch',
    #     python_callable=_branch
    # )
 
    beta = PythonOperator(
        task_id='beta',
        python_callable=_beta,
        provide_context=True,
    )

    plot = PythonOperator(
        task_id='plot',
        python_callable=_plot,
        provide_context=True,
    )
 
    # t3 = BashOperator(
    #     task_id='t3',
    #     bash_command="echo ''"
    # )

    # t4 = BashOperator(
    #         task_id='t4',
    #         bash_command="echo ''",
    #         trigger_rule='one_success'
    #     )

    wipe_old_csv_ifexists >> fetch_financial_data >> fetch_market_data >> [beta, plot] # >> [t2, t3] >> t4