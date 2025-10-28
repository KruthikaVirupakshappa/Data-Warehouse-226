
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



"""## Function to create ML forecast in snowflake to predict the next 7 days of the stock price"""
@task
def train(train_input_table, train_view, forecast_function_name):
    conn = return_snowflake_conn()
    try:
        cursor = conn.cursor()
        create_view_sql = f"""CREATE OR REPLACE VIEW {train_view} AS SELECT
            SYMBOL, DATE, CLOSE
            FROM {train_input_table};"""

        create_model_sql = f"""CREATE OR REPLACE SNOWFLAKE.ML.FORECAST {forecast_function_name} (
            INPUT_DATA => SYSTEM$REFERENCE('VIEW', '{train_view}'),
            SERIES_COLNAME => 'SYMBOL',
            TIMESTAMP_COLNAME => 'DATE',
            TARGET_COLNAME => 'CLOSE',
            CONFIG_OBJECT => {{ 'ON_ERROR': 'SKIP' }}
        );"""

        cursor.execute(create_view_sql)
        cursor.execute(create_model_sql)
        cursor.execute(f"CALL {forecast_function_name}!SHOW_EVALUATION_METRICS();")
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()

@task
def predict(forecast_function_name, train_input_table, forecast_table, final_table):
    try:
        conn = return_snowflake_conn()
        cursor = conn.cursor()
        make_prediction_sql = f"""BEGIN
            CALL {forecast_function_name}!FORECAST(
                FORECASTING_PERIODS => 7,
                CONFIG_OBJECT => {{'prediction_interval': 0.95}}
            );
            LET x := SQLID;
            CREATE OR REPLACE TABLE {forecast_table} AS SELECT * FROM TABLE(RESULT_SCAN(:x));
        END;"""
        create_final_table_sql = f"""CREATE OR REPLACE TABLE {final_table} AS
            SELECT SYMBOL, DATE, CLOSE AS actual, NULL AS forecast, NULL AS lower_bound, NULL AS upper_bound
            FROM {train_input_table}
            UNION ALL
            SELECT replace(series, '"', '') as SYMBOL, ts as DATE, NULL AS actual, forecast, lower_bound, upper_bound
            FROM {forecast_table};"""

        cursor.execute(make_prediction_sql)
        cursor.execute(create_final_table_sql)
        conn.commit()
    except Exception as e:
        conn.rollback()
        raise
    finally:
        conn.close()


with DAG(
    dag_id='stocks_prediction_analysis',
    start_date=datetime(2025, 10, 5),
    catchup=False,
    tags=['ML', 'ELT'],
    schedule_interval='30 2 * * *', 
) as dag:

    train_input_table = "raw.stock_prices"
    train_view = "raw.train_data_view"
    forecast_table = "raw.stock_forecast"
    forecast_function_name = "analytics.predict_stock_price"
    final_table = "analytics.final_result"
    tickers = ["NVDA","TSLA"]
    
    records = download_stock_data(tickers, period="180d")
    load_data = load_records(train_input_table, records)
    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)

    records >> load_data >> train_task >> predict_task

