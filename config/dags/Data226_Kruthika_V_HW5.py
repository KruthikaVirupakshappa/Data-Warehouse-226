from airflow import DAG
from airflow.models import Variable
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import requests

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@task
def return_last_90d_price(symbol):
    vantage_api_key = Variable.get('vantage_api_key')
    url = f'https://www.alphavantage.co/query?function=TIME_SERIES_DAILY&symbol={symbol}&apikey={vantage_api_key}'
    r = requests.get(url, timeout=30)
    data = r.json()
    if "Time Series (Daily)" not in data:
        raise Exception(f"Alpha Vantage error: {data.get('Note') or data.get('Error Message') or 'unknown'}")
    results = []
    for d in sorted(data["Time Series (Daily)"].keys(), reverse=True):
        if len(results) == 90:
            break
        record = data["Time Series (Daily)"][d]
        record["date"] = d
        results.append(record)
    return results

def create_table(table):
    conn = return_snowflake_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(
                f"""CREATE TABLE IF NOT EXISTS {table} (
                    symbol VARCHAR, open FLOAT, high FLOAT, low FLOAT, close FLOAT,
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
def load_records(table, results, symbol):
    conn = return_snowflake_conn()
    try:
        create_table(table) 
        cur = conn.cursor()
        try:
            for r in results:
                open_price = r["1. open"]
                high = r["2. high"]
                low = r["3. low"]
                close = r["4. close"]
                volume = r["5. volume"]
                date = r["date"]

                merge_sql = f"""
                MERGE INTO {table} t
                USING (SELECT %s AS symbol,
                              %s AS open,
                              %s AS high,
                              %s AS low,
                              %s AS close,
                              %s AS volume,
                              TO_DATE(%s) AS date) s
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
                cur.execute(merge_sql, (symbol, open_price, high, low, close, volume, date))
        finally:
            cur.close()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

@task
def count_records(table):
    conn = return_snowflake_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute(f"SELECT COUNT(*) FROM {table}")
            count = cur.fetchone()[0]
        finally:
            cur.close()
        conn.commit()
        print(f"Row count: {count}")
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


with DAG(
    dag_id='Stock_analysis',
    start_date=datetime(2025, 10, 2),
    catchup=False,
    tags=['ETL'],
    schedule_interval='30 2 * * *', 
) as dag:
    target_table = "raw.NVDA_DATA"
    symbol = "NVDA"

    results = return_last_90d_price(symbol)
    load1 = load_records(target_table, results, symbol)
    print("Records on first run")
    count1 = count_records(target_table)
    load2 = load_records(target_table, results, symbol)
    print("Records on second run")
    count2 = count_records(target_table)

    results >> load1 >> count1 >> load2 >> count2
