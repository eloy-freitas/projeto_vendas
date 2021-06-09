import pandas as pd
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_forma_pagamento(conn):
    dim_forma_pagamento = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_FORMA_PAGAMENTO"'
    )

    return dim_forma_pagamento


def treat_dim_forma_pagamento(dim_forma_pagamento):
    columns_names = {
        "id_pagamento": "CD_FORMA_PAGAMENTO",
        "nome": "NO_FORMA_PAGAMENTO",
        "descricao": "DS_FORMA_PAGAMENTO"
    }

    dim_forma_pagamento = (
        dim_forma_pagamento.
            rename(columns=columns_names).
            assign(DS_FORMA_PAGAMENTO=lambda x: x.DS_FORMA_PAGAMENTO.
                   apply(lambda y:
                         y[:-1].upper()
                         if y.endswith(",") or y.endswith(".")
                         else y.upper()))
    )

    dim_forma_pagamento. \
        insert(0,
               'SK_FORMA_PAGAMENTO',
               range(1, 1 + len(dim_forma_pagamento)))

    dim_forma_pagamento = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado"],
            [-2, -2, "Não aplicável", "Não aplicável"],
            [-3, -3, "Desconhecido", "Desconhecido"]
        ], columns=dim_forma_pagamento.columns).
            append(dim_forma_pagamento)
    )

    return dim_forma_pagamento


def load_dim_forma_pagamento(dim_forma_pagamento, conn):
    insert_data(
        data=dim_forma_pagamento,
        connection=conn,
        table_name='D_FORMA_PAGAMENTO',
        schema_name='DW',
        action='replace'
    )


def run_dim_forma_pagamento(conn):
    (
        extract_dim_forma_pagamento(conn).
            pipe(treat_dim_forma_pagamento).
            pipe(load_dim_forma_pagamento, conn=conn)
    )


if __name__ == "__main__":
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    start = t.time()
    run_dim_forma_pagamento(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")