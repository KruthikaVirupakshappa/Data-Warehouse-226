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
            cur.execute(f"TRUNCATE TABLE {table}")
            rows_to_insert = [
                (
                    symbol,
                    r["1. open"],
                    r["2. high"],
                    r["3. low"],
                    r["4. close"],
                    r["5. volume"],
                    r["date"]
                ) for r in results
            ]
            insert_sql = f"""
                INSERT INTO {table} (symbol, open, high, low, close, volume, date) 
                VALUES (%s, %s, %s, %s, %s, %s, TO_DATE(%s))
            """
            cur.executemany(insert_sql, rows_to_insert)
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
        print(f"Row count in {table}: {count}")
        return count
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


with DAG(
    dag_id='Stock_analysis_Full_Refresh',
    start_date=datetime(2025, 10, 2),
    catchup=False,
    tags=['ETL'],
    schedule_interval='30 2 * * *', 
) as dag:
    target_table = "raw.NVDA_DATA"
    symbol = "NVDA"

    results = return_last_90d_price(symbol)
    load_data = load_records(target_table, results, symbol)
    count_data = count_records(target_table)

    results >> load_data >> count_data
