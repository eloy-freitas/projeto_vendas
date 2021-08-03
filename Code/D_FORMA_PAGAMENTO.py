import pandas as pd
import time as t
from pandasql import sqldf
from CONEXAO import create_connection_postgre
from sqlalchemy.types import String, Integer
import DW_TOOLS as dwt


def extract_stg_forma_pagamento(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão forma pagamento

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_forma_pagamento -- pandas.Dataframe;
    """
    stg_forma_pagamento = dwt.read_table(
        conn=conn,
        schema="STAGE",
        table_name="STG_FORMA_PAGAMENTO",
        columns=[
            "id_pagamento",
            "nome",
            "descricao"
        ]
    )

    return stg_forma_pagamento


def extract_dim_forma_pagamento(conn):
    """
    Extrai os registros da dim_forma_pagamento do DW

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_forma_pagamento -- dataframe da dim_forma_pagamento
    """
    try:
        dim_forma_pagamento = dwt.read_table(
            conn=conn,
            schema='DW',
            table_name='D_FORMA_PAGAMENTO',
            columns=[
                'SK_FORMA_PAGAMENTO',
                'CD_FORMA_PAGAMENTO',
                'NO_FORMA_PAGAMENTO',
                'DS_FORMA_PAGAMENTO'
            ]
        )

        return dim_forma_pagamento
    except:
        return None


def extract_new_forma_pagamento(conn):
    """
    Extrai novos registros na stage

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    new_values -- dataframe com as atualizações
    """
    dim_forma_pagamento = extract_dim_forma_pagamento(conn)

    stg_forma_pagamento = extract_stg_forma_pagamento(conn)

    new_funcionarios = (
        sqldf('\
                SELECT * FROM \
                stg_forma_pagamento stg \
                LEFT JOIN dim_forma_pagamento dim \
                ON stg.id_pagamento = dim.CD_FORMA_PAGAMENTO \
                WHERE dim.CD_FORMA_PAGAMENTO IS NULL')
    )

    new_values = (
        new_funcionarios.assign(
            df_size=dim_forma_pagamento['SK_FORMA_PAGAMENTO'].max() + 1
        )
    )

    return new_values


def treat_new_forma_pagamento(new_values):
    """
    Faz o tratamento dos novos registros encontrados na stage

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;
    new_values -- novos registros ou registros atualizados no formato pandas.Dataframe;

    return:
    dim_forma_pagamento -- dataframe dos novos registros tratados;
    """
    columns_names = {
        "id_pagamento": "CD_FORMA_PAGAMENTO",
        "nome": "NO_FORMA_PAGAMENTO",
        "descricao": "DS_FORMA_PAGAMENTO"
    }

    select_columns = [
        "id_pagamento",
        "nome",
         "descricao"
    ]

    dim_forma_pagamento = (
        new_values.
            filter(select_columns).
            rename(columns=columns_names).
            assign(DS_FORMA_PAGAMENTO=lambda x: x.DS_FORMA_PAGAMENTO.
                   apply(lambda y:
                         y[:-1].upper()
                         if y.endswith(",") or y.endswith(".")
                         else y.upper()))
    )

    size = new_values['df_size'].max()
    dim_forma_pagamento. \
        insert(0,
               'SK_FORMA_PAGAMENTO',
               range(size, size + len(dim_forma_pagamento)))

    return dim_forma_pagamento


def treat_dim_forma_pagamento(stg_forma_pagamento):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_forma_pagamento -- pandas.Dataframe;

    return:
    dim_forma_pagamento -- pandas.Dataframe;
    """
    columns_names = {
        "id_pagamento": "CD_FORMA_PAGAMENTO",
        "nome": "NO_FORMA_PAGAMENTO",
        "descricao": "DS_FORMA_PAGAMENTO"
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


def load_dim_forma_pagamento(dim_forma_pagamento, conn, action):
    """
    Faz a carga da dimensão forma pagamento no DW.

    parâmetros:
    dim_forma_pagamento -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    data_types = {
        "SK_FORMA_PAGAMENTO": Integer(),
        "CD_FORMA_PAGAMENTO": Integer(),
        "NO_FORMA_PAGAMENTO": String(),
        "DS_FORMA_PAGAMENTO": String()
    }
    (
        dim_forma_pagamento.
            astype('string').
            to_sql(
            con=conn,
            name='D_FORMA_PAGAMENTO',
            schema='DW',
            if_exists=action,
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )


def run_dim_forma_pagamento(conn):
    """
    Executa o pipeline da dimensão forma de pagamento.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    dim_forma_pagamento = extract_dim_forma_pagamento(conn)
    if dim_forma_pagamento is None:
        (
            extract_stg_forma_pagamento(conn).
                pipe(treat_dim_forma_pagamento).
                pipe(load_dim_forma_pagamento, conn=conn, action='replace')
        )
    else:
        (
            extract_new_forma_pagamento(conn).
                pipe(treat_new_forma_pagamento).
                pipe(load_dim_forma_pagamento, conn=conn, action='append')
        )


if __name__ == '__main__':
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )
    start = t.time()
    run_dim_forma_pagamento(conn_dw)
    print(f'exec time = {t.time() - start}')