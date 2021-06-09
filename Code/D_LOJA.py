import pandas as pd
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_loja(conn):
    dim_loja = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_LOJA";'
    )

    return dim_loja


def treat_dim_loja(dim_loja):
    columns_names = {"id_loja": "CD_LOJA",
                     "nome_loja": "NO_LOJA",
                     "razao_social": "DS_RAZAO_SOCIAL",
                     "cnpj": "NU_CNPJ",
                     "telefone": "NU_TELEFONE",
                     "id_endereco": "CD_ENDERECO_LOJA"
                     }
    dim_loja = (
        dim_loja.
        rename(columns=columns_names).
        assign(
            CD_LOJA=lambda x: x.CD_LOJA.astype("int64"),
            CD_ENDERECO_LOJA=lambda x: x.CD_ENDERECO_LOJA.astype("int64")
        )
    )

    dim_loja.insert(0, 'SK_LOJA', range(1, 1 + len(dim_loja)))

    dim_loja = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", -1, -1, -1],
            [-2, -2, "Não aplicável", "Não aplicável", -2, -2, -2],
            [-3, -3, "Desconhecido", "Desconhecido", -3, -3, -3]
        ], columns=dim_loja.columns).append(dim_loja)
    )

    return dim_loja


def load_dim_loja(dim_loja, conn):
    insert_data(dim_loja, conn, 'D_LOJA', 'DW', 'replace')


def run_dim_loja(conn):
    (
        extract_dim_loja(conn=conn).
        pipe(treat_dim_loja).
        pipe(load_dim_loja, conn=conn)
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
    run_dim_loja(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
