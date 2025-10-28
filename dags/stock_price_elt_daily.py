
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import yfinance as yf

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

# Function to download stock data
@task
def download_stock_data(tickers, period="180d"):
  data = yf.download(tickers, period=period)
  return data


# Function to create table
def create_table(table):
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


# Function to insert data into the table
@task
def load_records(table, results):
    try:
        conn = return_snowflake_conn()
        create_table(table)
        cursor = conn.cursor()
        try:
            symbols = results.columns.get_level_values('Ticker').unique()
            for date, row in results.iterrows():
                for symbol in symbols:
                    open_price = row["Open"][symbol]
                    high = row["High"][symbol]
                    low = row["Low"][symbol]
                    close = row["Close"][symbol]
                    volume = row["Volume"][symbol]
                    date_str = date.strftime('%Y-%m-%d')


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
    dag_id='stock_price_elt_daily',
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=['ELT'],
    schedule_interval='30 2 * * *', 
) as dag:

    train_input_table = "raw.stock_prices"
    tickers = ["NVDA","TSLA"]
    
    records = download_stock_data(tickers, period="180d")
    load_data = load_records(train_input_table, records)

    records >> load_data 

