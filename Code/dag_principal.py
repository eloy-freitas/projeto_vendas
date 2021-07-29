from airflow import DAG
from datetime import datetime
from airflow.operators.python import PythonOperator
from D_CLIENTE import run_dim_cliente
from D_FUNCIONARIO import run_dim_funcionario
from D_LOJA import run_dim_loja
from D_DATA import run_dim_data
from D_FORMA_PAGAMENTO import run_dim_forma_pagamento
from D_PRODUTO import run_dim_produto
from F_VENDA import run_fact_venda
from CONEXAO import create_connection_postgre
import STAGES as stg

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

    create_stg_cliente = PythonOperator(
        task_id='create_stg_cliente',
        python_callable=stg.create_stg_cliente,
        op_args={'conn': conn_dw}
    )

    create_stg_forma_pagamento = PythonOperator(
        task_id='create_stg_forma_pagamento',
        python_callable=stg.create_stg_forma_pagamento,
        op_args={'conn': conn_dw}
    )

    create_stg_funcionario = PythonOperator(
        task_id='create_stg_funcionario',
        python_callable=stg.create_stg_funcionario,
        op_args={'conn': conn_dw}
    )

    create_stg_endereco = PythonOperator(
        task_id='create_stg_endereco',
        python_callable=stg.create_stg_endereco,
        op_args={'conn': conn_dw}
    )

    create_stg_loja = PythonOperator(
        task_id='create_stg_loja',
        python_callable=stg.create_stg_loja,
        op_args={'conn': conn_dw}
    )

    create_stg_produto = PythonOperator(
        task_id='create_stg_produto',
        python_callable=stg.create_stg_produto,
        op_args={'conn', conn_dw}
    )

    create_stg_venda = PythonOperator(
        task_id='create_stg_venda',
        python_callable=stg.create_stg_venda,
        op_args={'conn': conn_dw}
    )

    create_stg_item_venda = PythonOperator(
        task_id='create_stg_item_venda',
        python_callable=stg.create_stg_venda,
        op_args={'conn': conn_dw}
    )

    task_run_dim_cliente = PythonOperator(
        task_id="run_dim_cliente",
        python_callable=run_dim_cliente,
        op_args={'conn': conn_dw}
    )

    task_run_dim_funcionario = PythonOperator(
        task_id="run_dim_funcionario",
        python_callable=run_dim_funcionario,
        op_args={'conn': conn_dw}
    )

    task_run_dim_data = PythonOperator(
        task_id='run_dim_data',
        python_callable=run_dim_data,
        op_args={'conn': conn_dw}
    )

    task_run_dim_loja = PythonOperator(
        task_id='run_dim_loja',
        python_callable=run_dim_loja,
        op_args={'conn': conn_dw}
    )

    task_run_dim_forma_pagamento = PythonOperator(
        task_id='run_dim_forma_pagamento',
        python_callable=run_dim_forma_pagamento,
        op_args={'conn': conn_dw}
    )

    task_run_dim_produto = PythonOperator(
        task_id='run_dim-produto',
        python_callable=run_dim_produto,
        op_args={'conn': conn_dw}
    )

    task_run_fact_venda = PythonOperator(
        task_id='run_fact_venda',
        python_callable=run_fact_venda,
        op_args={'conn': conn_dw}
    )

    task_run_dim_loja.set_downstream([
        create_stg_endereco,
        create_stg_loja])
    task_run_dim_cliente.set_downstream([
        create_stg_endereco,
        create_stg_cliente
    ])
    task_run_dim_produto.set_downstream(create_stg_produto)
    task_run_dim_funcionario.set_downstream(create_stg_funcionario)
    task_run_dim_forma_pagamento.set_downstream(create_stg_forma_pagamento)
    task_run_fact_venda.set_downstream([
        create_stg_venda,
        create_stg_item_venda,
        task_run_dim_produto,
        task_run_dim_data,
        task_run_dim_forma_pagamento,
        task_run_dim_funcionario,
        task_run_dim_cliente,
        task_run_dim_loja
    ])