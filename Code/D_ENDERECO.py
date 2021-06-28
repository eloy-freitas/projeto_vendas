import pandas as pd
import datetime as dt
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data
import DW_TOOLS as dwt


def extract_dim_endereco(conn):
    dim_endereco = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_ENDERECO'
    )

    return dim_endereco


def treat_dim_endereco(dim_endereco):
    columns_names = {
        "id_endereco": "CD_ENDERECO",
        "estado": "NO_ESTADO",
        "cidade": "NO_CIDADE",
        "bairro": "NO_BAIRRO",
        "rua": "DS_RUA"
    }

    select_columns = [
           "id_endereco",
           "estado",
           "cidade",
           "bairro",
           "rua"
    ]

    dim_endereco = (
        dim_endereco.
            filter(select_columns).
            rename(columns=columns_names).
            assign(
            DS_RUA=lambda x: x.DS_RUA.apply(
                lambda y: y.strip())
        )
    )

    dim_endereco.insert(
        loc=0,
        column='SK_ENDERECO',
        value=range(1, 1 + len(dim_endereco))
    )

    dim_endereco = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", "Não informado", "Não informado"],
            [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável"],
            [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido"]
        ], columns=dim_endereco.columns).append(dim_endereco)
    )

    return dim_endereco


def load_dim_endereco(dim_endereco, conn):
    dim_endereco.to_sql(
        con=conn,
        name='D_ENDERECO',
        schema='DW',
        if_exists='replace',
        index=False,
        chunksize=100
    )


def run_dim_endereco(conn):
    (
        extract_dim_endereco(conn).
            pipe(treat_dim_endereco).
            pipe(load_dim_endereco, conn=conn)
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
    run_dim_endereco(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
