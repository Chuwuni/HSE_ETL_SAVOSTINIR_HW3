from airflow import DAG
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
from sqlalchemy import create_engine

def load_incr_data():
    hook = PostgresHook(postgres_conn_id='target_db')
    
    conn = hook.get_connection('target_db')
    db_uri = f"postgresql://{conn.login}:{conn.password}@{conn.host}:{conn.port}/{conn.schema}"
    engine = create_engine(db_uri)
    
    import pandas as pd
    df = pd.read_csv('/opt/airflow/data/increment_data.csv')
    
    df.to_sql('orders_stg', con=engine, schema='public', if_exists='replace', index=False)
    
    upsert_sql = """
    INSERT INTO public.orders (order_id, customer_id, amount, order_date, status)
    SELECT order_id, customer_id, amount, order_date::DATE, status
    FROM public.orders_stg
    ON CONFLICT (order_id) DO UPDATE SET
        customer_id = EXCLUDED.customer_id,
        amount = EXCLUDED.amount,
        order_date = EXCLUDED.order_date,
        status = EXCLUDED.status;
        
    DROP TABLE public.orders_stg;
    """
    hook.run(upsert_sql)
    print(f"Upserted {len(df)} rows from increment.")


with DAG(
    dag_id='hw3_incr_load',
    schedule_interval=None,
    start_date=datetime(2026, 2, 1),
    catchup=False,
) as dag:
    
    load_incr_data = PythonOperator(
        task_id='load_incr_data',
        python_callable=load_incr_data
    )
