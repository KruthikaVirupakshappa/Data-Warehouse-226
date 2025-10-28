from airflow import DAG
from airflow.decorators import task
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook
from datetime import datetime

def return_snowflake_conn():
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')
    return hook.get_conn()

@task
def create_summary_table():
    conn = return_snowflake_conn()
    try:
        cur = conn.cursor()
        try:
            cur.execute("CREATE SCHEMA IF NOT EXISTS analytics")

            #Validate duplicates before publishing
            cur.execute("""
                SELECT st.sessionId, COUNT(*) AS cnt
                FROM raw.user_session_channel uc
                JOIN raw.session_timestamp st
                  ON uc.sessionId = st.sessionId
                GROUP BY st.sessionId
                HAVING COUNT(*) > 1
            """)
            duplicates = cur.fetchall()
            if duplicates:
                raise Exception(f"Duplicate records found: {duplicates}")

            # 3) create table using CTAS
            cur.execute("""
                CREATE OR REPLACE TABLE analytics.session_summary AS
                SELECT
                    uc.userId,
                    uc.sessionId,
                    uc.channel,
                    st.ts,
                    CAST(st.ts AS DATE) AS session_date,
                    CAST(DATE_TRUNC('WEEK', st.ts) AS DATE) AS week_start
                FROM raw.user_session_channel uc
                JOIN raw.session_timestamp st
                  ON uc.sessionId = st.sessionId
            """)
        finally:
            cur.close()
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()

with DAG(
    dag_id='Build_Snowflake_Summary_Table',
    start_date=datetime(2025, 10, 28),
    schedule_interval='30 2 * * *',
    catchup=False,
    tags=['Snowflake', 'S3', 'ETL'],
) as dag:
    create_summary_task = create_summary_table()
    create_summary_task
