from airflow import DAG
from airflow.decorators import task
from airflow.models import Variable
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime
import requests

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@task
def create_table_task(table):
    conn = return_snowflake_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""
                CREATE OR REPLACE TABLE {table} 
                (symbol VARCHAR, open FLOAT, high FLOAT, low FLOAT, close FLOAT, 
                volume INT, date DATE, PRIMARY KEY (symbol, date)
                )"""
            )
        finally:
            cur.close()
        conn.commit() 
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

@task
def download_stock_data(symbol):
    vantage_api_key = Variable.get('vantage_api_key')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    
    r = requests.get(url)
    data = r.json()
    
    results = []
    if "Time Series (Daily)" in data:
        time_series = data["Time Series (Daily)"]
        for date_str, values in time_series.items():
            results.append({
                'symbol': symbol,
                'date': date_str,
                'open': float(values['1. open']),
                'high': float(values['2. high']),
                'low': float(values['3. low']),
                'close': float(values['4. close']),
                'volume': int(values['5. volume'])
            })
    else:
        print(f"Warning: No data found for {symbol} or API limit reached.")
        print(f"API Response: {data}")
        
    return results

@task
def load_records(table, results_list):
    if not results_list:
        print("No records to load.")
        return

    try:
        conn = return_snowflake_conn()
        cursor = conn.cursor()
        try:
            for row in results_list:
                symbol = row['symbol']
                open_price = row['open']
                high = row['high']
                low = row['low']
                close = row['close']
                volume = row['volume']
                date_str = row['date']

                merge_sql = f"""
                MERGE INTO {table} t
                USING (SELECT %s AS symbol,
                            %s AS open,
                            %s AS high,
                            %s AS low,
                            %s AS close,
                            %s AS volume,
                            %s AS date) s
                ON t.symbol = s.symbol AND t.date = s.date
                WHEN MATCHED THEN UPDATE SET
                    t.open = s.open,
                    t.high = s.high,
                    t.low = s.low,
                    t.close = s.close,
                    t.volume = s.volume
                WHEN NOT MATCHED THEN INSERT (symbol, open, high, low, close, volume, date)
                VALUES (s.symbol, s.open, s.high, s.low, s.close, s.volume, s.date)
                """
                cursor.execute(merge_sql, (symbol, open_price, high, low, close, volume, date_str))
        finally:
            cursor.close()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

with DAG(
    dag_id='stock_price_etl_vantage',
    start_date=datetime(2025, 11, 23),
    catchup=False,
    tags=['ETL'],
    schedule='30 2 * * *', 
) as dag:

    train_input_table = "raw.stock_prices"
    symbols = ["NVDA", "TSLA"]
    setup = create_table_task(train_input_table)

    for symbol in symbols:
        extracted_data = download_stock_data.override(task_id=f'download_{symbol}')(symbol)
        load_task = load_records.override(task_id=f'load_{symbol}')(train_input_table, extracted_data)

        setup >> load_task