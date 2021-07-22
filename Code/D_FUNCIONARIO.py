import pandas as pd
import time as t
from sqlalchemy.types import Date, String, Integer
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt


def extract_dim_funcionario(conn):
    dim_funcionario = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STG_FUNCIONARIO'
    )

    return dim_funcionario


def treat_dim_funcionario(dim_funcionario):
    columns_names = {
        "id_funcionario": "CD_FUNCIONARIO",
        "nome": "NO_FUNCIONARIO",
        "cpf": "NU_CPF",
        "tel": "NU_TELEFONE",
        "data_nascimento": "DT_NASCIMENTO"
    }

    select_columns = [
        "id_funcionario",
        "nome",
        "cpf",
        "tel",
        "data_nascimento"
    ]

    dim_funcionario = (
        dim_funcionario.
            filter(select_columns).
            rename(columns=columns_names).
            assign(
            DT_NASCIMENTO=lambda x: x.DT_NASCIMENTO.astype('datetime64'))
    )

    dim_funcionario.insert(0, 'SK_FUNCIONARIO', range(1, 1 + len(dim_funcionario)))

    dim_funcionario = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, None],
            [-2, -2, "Não aplicável", -2, -2, None],
            [-3, -3, "Desconhecido", -3, -3, None]
        ], columns=dim_funcionario.columns).append(dim_funcionario)
    )

    return dim_funcionario


def load_dim_funcionario(dim_funcionario, conn):
    data_types = {
        "SK_FUNCIONARIO": Integer(),
        "CD_FUNCIONARIO": Integer(),
        "NO_FUNCIONARIO": String(),
        "NU_CPF": String(),
        "NU_TELEFONE": String(),
        "DT_NASCIMENTO": Date()
    }

    (
        dim_funcionario.
            astype('string').
            to_sql(
            con=conn,
            name='D_FUNCIONARIO',
            schema='DW',
            if_exists='replace',
            index=False,
            chunksize=100,
            dtype=data_types
        )

    )


def run_dim_funcionario(conn):
    (
        extract_dim_funcionario(conn).
            pipe(treat_dim_funcionario).
            pipe(load_dim_funcionario, conn=conn)
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
    run_dim_funcionario(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
