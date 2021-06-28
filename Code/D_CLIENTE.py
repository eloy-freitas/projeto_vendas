import pandas as pd
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data
import DW_TOOLS as dwt


def extract_dim_cliente(conn):
    dim_cliente = dwt.read_table(
        conn=conn,
        schema="STAGE",
        table_name="STAGE_CLIENTE",
    )

    return dim_cliente


def treat_dim_cliente(dim_cliente):
    columns_name = {
        "id_cliente": "CD_CLIENTE",
        "nome": "NO_CLIENTE",
        "cpf": "NU_CPF",
        "tel": "NU_TELEFONE",
        "id_endereco": "CD_ENDERECO_CLIENTE"
    }
    select_columns = [
        "id_cliente",
        "nome",
        "cpf",
        "tel"
    ]

    dim_cliente = (
        dim_cliente.
            filter(select_columns).
            rename(columns=columns_name).
            assign(
            NU_TELEFONE=lambda x: x.NU_TELEFONE.apply(
                lambda y: y[0:8] + y[-5:]
            )).
            assign(
            CD_CLIENTE=lambda x: x.CD_CLIENTE.astype('int64')
        )
    )

    dim_cliente.insert(0, 'SK_CLIENTE', range(1, 1 + len(dim_cliente)))

    dim_cliente = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1],
            [-2, -2, "Não aplicável", -2, -2],
            [-3, -3, "Desconhecido", -3, -3]
        ], columns=dim_cliente.columns).append(dim_cliente)
    )

    return dim_cliente


def load_dim_cliente(dim_cliente, conn):
    dim_cliente.to_sql(
        con=conn,
        name='D_CLIENTE',
        schema='DW',
        if_exists='replace',
        index=False,
        chunksize=100
    )


def run_dim_cliente(conn):
    (
        extract_dim_cliente(conn).
            pipe(treat_dim_cliente).
            pipe(load_dim_cliente, conn=conn)
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
    run_dim_cliente(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
