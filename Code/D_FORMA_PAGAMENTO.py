import pandas as pd
import time as t
from pandasql import sqldf
from sqlalchemy.types import String, Integer
from Code.CONEXAO import create_connection_postgre
import Code.DW_TOOLS as dwt


def extract_dim_forma_pagamento(conn):
    """
    Extrai os dados da stage forma pagamento

    :parameter:
        conn -- sqlalchemy.engine;

    :return:
        stg_forma_pagamento -- pandas.Dataframe;
    """
    stg_forma_pagamento = dwt.read_table(
        conn=conn,
        schema="stage",
        table_name="stg_forma_pagamento",
        columns=[
            "id_pagamento",
            "nome",
            "descricao"
        ]
    )

    if dwt.verify_table_exists(conn=conn, table='d_forma_pagamento', schema='dw'):
        query = """
            SELECT
                stg.id_pagamento,
                stg.nome,
                stg.descricao 
            FROM stg_forma_pagamento stg
            LEFT JOIN dw.d_forma_pagamento dim
            ON stg.id_pagamento = dim.cd_forma_pagamento
            WHERE dim.cd_forma_pagamento IS NULL
        """
        stg_forma_pagamento = sqldf(query, {'stg_forma_pagamento': stg_forma_pagamento}, conn.url)

    return stg_forma_pagamento


def treat_dim_forma_pagamento(stg_forma_pagamento, conn):
    """
    Faz o tratamento dos novos registros encontrados na stage

    :parameter:
        conn -- sqlalchemy.engine;
    :parameter:
        stg_forma_pagamento -- pandas.Dataframe;

    :return:
        dim_forma_pagamento -- pandas.Dataframe;
    """
    columns_names = {
        "id_pagamento": "cd_forma_pagamento",
        "nome": "no_forma_pagamento",
        "descricao": "ds_forma_pagamento"
    }

    select_columns = [
        "id_pagamento",
        "nome",
        "descricao"
    ]

    dim_forma_pagamento = (
        stg_forma_pagamento.
        filter(select_columns).
        rename(columns=columns_names).
        assign(
            ds_forma_pagamento=lambda x: x.ds_forma_pagamento.
            apply(
                lambda y: y[:-1].upper()
                if y.endswith(",") or y.endswith(".")
                else y.upper())
        )
    )

    if dwt.verify_table_exists(conn=conn, table='d_forma_pagamento', schema='dw'):
        size = dwt.find_max_sk(
            conn=conn,
            schema='dw',
            table='d_forma_pagamento',
            sk_name='sk_forma_pagamento'
        )

        dim_forma_pagamento.insert(0, 'sk_forma_pagamento', range(size, size + len(dim_forma_pagamento)))
    else:
        dim_forma_pagamento.insert(0, 'sk_forma_pagamento', range(1, 1 + len(dim_forma_pagamento)))

        dim_forma_pagamento = (
            pd.DataFrame([
                [-1, -1, "Não informado", "Não informado"],
                [-2, -2, "Não aplicável", "Não aplicável"],
                [-3, -3, "Desconhecido", "Desconhecido"]],
                columns=dim_forma_pagamento.columns).
            append(dim_forma_pagamento)
        )

    return dim_forma_pagamento


def load_dim_forma_pagamento(dim_forma_pagamento, conn):
    """
    Faz a carga da dimensão forma pagamento no DW.

    :parameter:
        dim_forma_pagamento -- pandas.Dataframe;
    :parameter:
        conn -- sqlalchemy.engine;
    """
    data_types = {
        "sk_forma_pagamento": Integer(),
        "cd_forma_pagamento": Integer(),
        "no_forma_pagamento": String(),
        "ds_forma_pagamento": String()
    }
    (
        dim_forma_pagamento.
        astype('string').
        to_sql(
            con=conn,
            name='d_forma_pagamento',
            schema='dw',
            if_exists='append',
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )


def run_dim_forma_pagamento(conn):
    """
    Executa o pipeline da dimensão forma de pagamento.

    :parameter:
        conn -- sqlalchemy.engine;
    """

    if dwt.verify_table_exists(conn=conn, schema='stage', table='stg_forma_pagamento'):
        tbl_pagamento = extract_dim_forma_pagamento(conn)

        if tbl_pagamento.shape[0] != 0:
            (
                treat_dim_forma_pagamento(stg_forma_pagamento=tbl_pagamento, conn=conn).
                pipe(load_dim_forma_pagamento, conn=conn)
            )


if __name__ == '__main__':
    conn = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )
    start = t.time()
    run_dim_forma_pagamento(conn)
    print(f'exec time = {t.time() - start}')
