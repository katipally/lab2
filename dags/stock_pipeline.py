from airflow import DAG
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from airflow.decorators import task
from statsmodels.tsa.arima.model import ARIMA
import numpy as np
import requests
import pandas as pd
from datetime import timedelta, datetime

def return_snowflake_conn():
    snowflake_hook = SnowflakeHook(
        snowflake_conn_id='snowflake_default'  
    )
    conn = snowflake_hook.get_conn()
    return conn.cursor()

@task
def extract_stock_data(stock_symbol):
    API_KEY = Variable.get('vantage_api_key')
    url = f"https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={stock_symbol}&apikey={API_KEY}"
    response = requests.get(url)
    data = response.json()["Time Series (Daily)"]
    
    df = pd.DataFrame.from_dict(data, orient='index')
    df.index = pd.to_datetime(df.index)
    df.columns = ['open', 'high', 'low', 'close', 'volume']
    df['symbol'] = stock_symbol
    
    df = df.loc[df.index >= (datetime.now() - timedelta(days=90))]
    df.reset_index(inplace=True)
    df.rename(columns={"index": "date"}, inplace=True)
    
    return df

@task
def load_data_to_snowflake(df):
    cur = return_snowflake_conn()
    try:
        for _, row in df.iterrows():
            check_query = f"SELECT COUNT(1) FROM raw_data.stock_prices WHERE date = '{row['date'].strftime('%Y-%m-%d')}' AND symbol = '{row['symbol']}'"
            cur.execute(check_query)
            exists = cur.fetchone()[0]

            if exists == 0:
                insert_query = f"""
                INSERT INTO raw_data.stock_prices (date, open, high, low, close, volume, symbol)
                VALUES ('{row['date'].strftime('%Y-%m-%d')}', {row['open']}, {row['high']}, {row['low']}, {row['close']}, {row['volume']}, '{row['symbol']}')
                """
                cur.execute(insert_query)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error occurred: {e}")
        raise e
    finally:
        cur.close()

@task
def fetch_data_from_snowflake(stock_symbol):
    cur = return_snowflake_conn()
    query = f"SELECT date, close FROM raw_data.stock_prices WHERE symbol = '{stock_symbol}' ORDER BY date"
    cur.execute(query)
    df = pd.DataFrame(cur.fetchall(), columns=['date', 'close'])
    cur.close()
    return df

@task
def predict_next_7_days(df, stock_symbol):
    df['date'] = pd.to_datetime(df['date'])
    df['close'] = df['close'].astype(float)
    df = df.sort_values(by='date')
    
    close_prices = df['close'].values
    model = ARIMA(close_prices, order=(5, 1, 0))
    model_fit = model.fit()
    forecast = model_fit.forecast(steps=7)
    
    last_date = df['date'].max()
    future_dates = [last_date + timedelta(days=i) for i in range(1, 8)]
    
    prediction_df = pd.DataFrame({
        'date': future_dates,
        'predicted_close': forecast
    })
    
    prediction_df['symbol'] = stock_symbol
    return prediction_df

@task
def load_forecast_to_snowflake(prediction_df):
    cur = return_snowflake_conn()
    try:
        for _, row in prediction_df.iterrows():
            check_query = f"SELECT COUNT(1) FROM raw_data.stock_forecasts WHERE date = '{row['date'].strftime('%Y-%m-%d')}' AND symbol = '{row['symbol']}'"
            cur.execute(check_query)
            exists = cur.fetchone()[0]

            if exists == 0:
                insert_query = f"""
                INSERT INTO raw_data.stock_forecasts (date, close, symbol)
                VALUES ('{row['date'].strftime('%Y-%m-%d')}', {row['predicted_close']}, '{row['symbol']}')
                """
                cur.execute(insert_query)
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK;")
        print(f"Error occurred: {e}")
        raise e
    finally:
        cur.close()

with DAG(
    dag_id='stock_pipeline',
    start_date=datetime(2024, 10, 10),
    catchup=False,
    schedule_interval='@daily',
    tags=['ETL']
) as dag:
    
    stock_symbol = ["AAPL", "MSFT"]

    for symbol in stock_symbol:
        stock_data = extract_stock_data(symbol)
        load_data_to_snowflake(stock_data)
        data_from_snowflake = fetch_data_from_snowflake(symbol)
        prediction_data = predict_next_7_days(data_from_snowflake, symbol)
        load_forecast_to_snowflake(prediction_data)
