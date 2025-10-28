
from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
import yfinance as yf

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()



# Function to create ML forecast in snowflake to predict the next 7 days of the stock price
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
    dag_id='stock_price_forecast_pipeline',
    start_date=datetime(2025, 10, 6),
    catchup=False,
    tags=['ML'],
    schedule_interval='30 3 * * *', 
) as dag:

    train_input_table = "raw.stock_prices"
    train_view = "raw.train_data_view"
    forecast_table = "raw.stock_forecast"
    forecast_function_name = "analytics.predict_stock_price"
    final_table = "analytics.final_result"
    tickers = ["NVDA","TSLA"]
    
    train_task = train(train_input_table, train_view, forecast_function_name)
    predict_task = predict(forecast_function_name, train_input_table, forecast_table, final_table)

    train_task >> predict_task

