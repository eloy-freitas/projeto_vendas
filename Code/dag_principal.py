from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from D_CLIENTE import run_dim_cliente
from CONEXAO import create_connection_postgre
from STAGES import create_stg_cliente

conn_dw = create_connection_postgre(
    server="192.168.3.2",
    database="projeto_dw_vendas",
    username="itix",
    password="itix123",
    port="5432"
)

with DAG("dag_principal",
         start_date=datetime(2021, 7, 28),
         schedule_interval="@daily", catchup=False) as dag:

    run_stg_cliente = PythonOperator(
        task_id='run_stg_cliente',
        python_callable=create_stg_cliente,
        op_args={'conn': conn_dw}
    )

    run_dim_cliente = PythonOperator(
        task_id="run_dim_cliente",
        python_callable=run_dim_cliente,
        op_args={'conn': conn_dw}
    )
