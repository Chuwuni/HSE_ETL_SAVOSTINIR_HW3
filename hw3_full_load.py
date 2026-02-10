from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_full_data():
    hook = PostgresHook(postgres_conn_id='target_db')
    
    create_sql = """
    DROP TABLE IF EXISTS public.orders;
    CREATE TABLE public.orders (
        order_id INT PRIMARY KEY,
        customer_id INT,
        amount NUMERIC,
        order_date DATE,
        status VARCHAR(20)
    );
    """
    hook.run(create_sql)
    
    conn = hook.get_connection('target_db')
    db_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(db_uri)
    
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/history_data.csv')
    df.to_sql('orders', con=engine, schema='public', if_exists='append', index=False)
    print(f"Loaded {len(df)} rows history.")


with DAG(
    dag_id='hw3_full_load',
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
) as dag:
    
    task_full_load = PythonOperator(
        task_id='load_hist_data',
        python_callable=load_full_data
    )
