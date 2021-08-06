import time as t
import pandas as pd
from sqlalchemy.types import Integer, String, Float
from pandasql import sqldf
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt

pd.set_option('display.max_columns', None)


def verify_fact_exists(conn):
    try:
        fact_venda = dwt.read_table(
            conn=conn,
            schema='DW',
            table_name='F_VENDA',
            columns=[
                'SK_FORMA_PAGAMENTO',
                'SK_CLIENTE',
                'SK_FUNCIONARIO',
                'SK_LOJA',
                'SK_PRODUTO',
                'SK_DT_VENDA',
                'NU_NFC',
                'QTD_PRODUTO',
                'VL_PRECO_CUSTO',
                'VL_PERCENTUAL_LUCRO'],
            where='"SK_LOJA" > 0 LIMIT 10'
        )

        return fact_venda
    except:
        return None


def extract_fact_venda(conn):
    fact_venda = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='F_VENDA',
        columns=[
            'SK_FORMA_PAGAMENTO',
            'SK_CLIENTE',
            'SK_FUNCIONARIO',
            'SK_LOJA',
            'SK_PRODUTO',
            'SK_DT_VENDA',
            'NU_NFC',
            'QTD_PRODUTO',
            'VL_PRECO_CUSTO',
            'VL_PERCENTUAL_LUCRO'
        ]
    )

    return fact_venda


def extract_dim_forma_pagamento(conn):
    dim_forma_pagamento = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_FORMA_PAGAMENTO',
        columns=["SK_FORMA_PAGAMENTO", "CD_FORMA_PAGAMENTO"]
    )

    return dim_forma_pagamento


def extract_dim_cliente(conn):
    dim_cliente = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_CLIENTE',
        columns=["SK_CLIENTE", "CD_CLIENTE"]
    )

    return dim_cliente


def extract_dim_funcionario(conn):
    dim_funcionario = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_FUNCIONARIO',
        columns=["SK_FUNCIONARIO", "CD_FUNCIONARIO"]
    )

    return dim_funcionario


def extract_dim_data(conn):
    dim_data = (
        dwt.read_table(
            conn=conn,
            schema='DW',
            table_name='D_DATA',
            columns=['SK_DATA', 'DT_REFERENCIA']).
            assign(
            DT_REFERENCIA=lambda x:
            pd.to_datetime(
                x.DT_REFERENCIA,
                format='%Y-%m-%d %H:%M:%S')).
            assign(
            DT_REFERENCIA=lambda x: x.DT_REFERENCIA.astype(str)).
            assign(
            DT_REFERENCIA=lambda x: x.DT_REFERENCIA.apply(
                lambda y: y[:13])
        )
    )
    return dim_data


def extract_dim_produto(conn):
    dim_produto = (
        dwt.read_table(
            conn=conn,
            schema='DW',
            table_name='D_PRODUTO',
            columns=["SK_PRODUTO", "CD_PRODUTO",
                     "VL_PRECO_CUSTO", "VL_PERCENTUAL_LUCRO",
                     "DT_INICIO", "DT_FIM", "FL_ATIVO"]).
            assign(
            DT_INICIO=lambda x:
            pd.to_datetime(
                x.DT_INICIO,
                format='%Y-%m-%d %H:%M:%S'),
            DT_FIM=lambda x:
            pd.to_datetime(
                x.DT_FIM,
                format='%Y-%m-%d %H:%M:%S'))
    )

    return dim_produto


def extract_dim_loja(conn):
    dim_loja = (
        dwt.read_table(
            conn=conn,
            schema='DW',
            table_name='D_LOJA',
            columns=[
                "SK_LOJA",
                "CD_LOJA",
                "DT_INICIO",
                "DT_FIM",
                "FL_ATIVO"]).
            assign(
            DT_INICIO=lambda x:
            pd.to_datetime(
                x.DT_INICIO,
                format='%Y-%m-%d %H:%M:%S'),
            DT_FIM=lambda x:
            pd.to_datetime(
                x.DT_FIM,
                format='%Y-%m-%d %H:%M:%S'))

    )

    return dim_loja


def extract_stage_venda(conn):
    stage_venda = (
        dwt.read_table(
            conn=conn,
            schema='STAGE',
            table_name='STG_VENDA',
            columns=['id_venda', 'id_pagamento', 'id_cliente',
                     'id_func', 'id_loja', 'nfc', 'data_venda']).
            assign(
            data_venda=lambda x: pd.to_datetime(
                x.data_venda,
                format='%Y-%m-%d %H:%M:%S')).
            assign(
            data_referencia=lambda x: x.data_venda.astype(str)).
            assign(
            data_referencia=lambda x: x.data_referencia.apply(
                lambda y: y[:13]),
            id_venda=lambda x: x.id_venda.astype('int64'),
            id_pagamento=lambda x: x.id_pagamento.astype('int64'),
            id_cliente=lambda x: x.id_cliente.astype('int64'),
            id_func=lambda x: x.id_func.astype('int64'),
            id_loja=lambda x: x.id_loja.astype('int64')
        ))

    stage_item_venda = (
        dwt.read_table(
            conn=conn,
            schema='STAGE',
            table_name='STG_ITEM_VENDA',
            columns=[
                'id_venda',
                'id_produto',
                'qtd_produto'
            ]).
            assign(
            id_venda=lambda x: x.id_venda.astype('int64')))

    stg_venda = (
        stage_venda.
            pipe(pd.merge,
                 right=stage_item_venda,
                 left_on="id_venda",
                 right_on="id_venda",
                 suffixes=["_11", "_12"])
    )

    return stg_venda


def extract_new_values(conn):
    stg_venda = extract_stage_venda(conn)

    fact_venda = extract_fact_venda(conn)

    new_values = (
        sqldf('\
                SELECT\
                stg.id_venda,\
                stg.id_pagamento,\
                stg.id_cliente,\
                stg.id_func,\
                stg.id_loja,\
                stg.nfc,\
                stg.data_venda,\
                stg.data_referencia,\
                stg.id_produto,\
                stg.qtd_produto\
                FROM stg_venda stg\
                LEFT JOIN fact_venda fact\
                ON fact.NU_NFC = stg.nfc\
                WHERE fact.NU_NFC IS NULL;'
              )
    )

    if len(new_values) > 0:
        dim_forma_pagamento = extract_dim_forma_pagamento(conn)
        dim_cliente = extract_dim_cliente(conn)
        dim_funcionario = extract_dim_funcionario(conn)
        dim_loja = extract_dim_loja(conn)
        dim_produto = extract_dim_produto(conn)
        dim_data = extract_dim_data(conn)

        fact_merged_dimensions = (
            new_values.
                pipe(dwt.merge_input,
                     right=dim_data,
                     left_on="data_referencia",
                     right_on="DT_REFERENCIA",
                     suff=['_16', '_17'],
                     surrogate_key='SK_DATA').
                pipe(dwt.merge_input,
                     right=dim_forma_pagamento,
                     left_on="id_pagamento",
                     right_on="CD_FORMA_PAGAMENTO",
                     suff=["_01", "_02"],
                     surrogate_key="SK_FORMA_PAGAMENTO").
                pipe(dwt.merge_input,
                     right=dim_cliente,
                     left_on="id_cliente",
                     right_on="CD_CLIENTE",
                     suff=["_03", "_04"],
                     surrogate_key="SK_CLIENTE").
                pipe(dwt.merge_input,
                     right=dim_funcionario,
                     left_on="id_func",
                     right_on="CD_FUNCIONARIO",
                     suff=["_05", "_06"],
                     surrogate_key="SK_FUNCIONARIO")
        )

    merge_with_produto = (
        sqldf('\
            SELECT\
            fmd.SK_FORMA_PAGAMENTO,\
            fmd.SK_CLIENTE,\
            fmd.SK_FUNCIONARIO,\
            fmd.SK_DATA,\
            fmd.data_venda,\
            fmd.qtd_produto,\
            fmd.nfc,\
            fmd.id_loja,\
            p.SK_PRODUTO,\
            p.VL_PRECO_CUSTO,\
            p.VL_PERCENTUAL_LUCRO\
            FROM fact_merged_dimensions fmd\
            LEFT JOIN dim_produto p\
            ON fmd.id_produto = p.CD_PRODUTO\
            WHERE p.CD_PRODUTO = fmd.id_produto AND p.FL_ATIVO = 1;'
              )
    )

    merge_with_loja = (
        sqldf(f'SELECT\
                mwp.SK_FORMA_PAGAMENTO,\
                mwp.SK_CLIENTE,\
                mwp.SK_FUNCIONARIO,\
                mwp.SK_DATA,\
                mwp.data_venda,\
                mwp.qtd_produto,\
                mwp.nfc,\
                mwp.SK_PRODUTO,\
                mwp.VL_PRECO_CUSTO,\
                mwp.VL_PERCENTUAL_LUCRO,\
                l.SK_LOJA\
                FROM merge_with_produto mwp\
                LEFT JOIN dim_loja l ON mwp.id_loja = l.CD_LOJA\
                WHERE l.CD_LOJA = mwp.id_loja AND l.FL_ATIVO = 1;'
              )
    )

    return merge_with_loja


def extract_new_venda(conn):
    """
    Extrai todas as tabelas necessárias para gerar a fato venda

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_cliente_endereco -- pandas.Dataframe;
    """
    stg_venda = extract_stage_venda(conn)

    dim_forma_pagamento = extract_dim_forma_pagamento(conn)

    dim_cliente = extract_dim_cliente(conn)

    dim_funcionario = extract_dim_funcionario(conn)

    dim_loja = extract_dim_loja(conn)

    dim_produto = extract_dim_produto(conn)

    dim_data = extract_dim_data(conn)

    stage_merged_dimensions = (
        stg_venda.
            pipe(dwt.merge_input,
                 right=dim_data,
                 left_on="data_referencia",
                 right_on="DT_REFERENCIA",
                 suff=['_16', '_17'],
                 surrogate_key='SK_DATA').
            pipe(dwt.merge_input,
                 right=dim_forma_pagamento,
                 left_on="id_pagamento",
                 right_on="CD_FORMA_PAGAMENTO",
                 suff=["_01", "_02"],
                 surrogate_key="SK_FORMA_PAGAMENTO").
            pipe(dwt.merge_input,
                 right=dim_cliente,
                 left_on="id_cliente",
                 right_on="CD_CLIENTE",
                 suff=["_03", "_04"],
                 surrogate_key="SK_CLIENTE").
            pipe(dwt.merge_input,
                 right=dim_funcionario,
                 left_on="id_func",
                 right_on="CD_FUNCIONARIO",
                 suff=["_05", "_06"],
                 surrogate_key="SK_FUNCIONARIO")
    )

    merge_with_produto = (
        sqldf('\
                SELECT\
                smd.SK_FORMA_PAGAMENTO,\
                smd.SK_CLIENTE,\
                smd.SK_FUNCIONARIO,\
                smd.SK_DATA,\
                smd.data_venda,\
                smd.qtd_produto,\
                smd.nfc,\
                smd.id_loja,\
                p.SK_PRODUTO,\
                p.VL_PRECO_CUSTO,\
                p.VL_PERCENTUAL_LUCRO\
                FROM stage_merged_dimensions smd\
                INNER JOIN dim_produto p\
                ON smd.id_produto = p.CD_PRODUTO\
                WHERE (p.DT_INICIO <= smd.data_venda < p.DT_FIM)\
                OR (p.DT_INICIO <= smd.data_venda AND p.DT_FIM IS NULL);')
    )

    merge_with_loja = (
        sqldf('\
                SELECT\
                mwp.SK_FORMA_PAGAMENTO,\
                mwp.SK_CLIENTE,\
                mwp.SK_FUNCIONARIO,\
                mwp.SK_DATA,\
                mwp.data_venda,\
                mwp.qtd_produto,\
                mwp.nfc,\
                mwp.SK_PRODUTO,\
                mwp.VL_PRECO_CUSTO,\
                mwp.VL_PERCENTUAL_LUCRO,\
                l.SK_LOJA\
                FROM merge_with_produto mwp\
                INNER JOIN dim_loja l\
                ON mwp.id_loja = l.CD_LOJA\
                WHERE (l.DT_INICIO <= mwp.data_venda < l.DT_FIM)\
                OR (l.DT_INICIO <= mwp.data_venda AND l.DT_FIM IS NULL);')
    )

    return merge_with_loja


def treat_fact_venda(stg_venda):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_venda -- pandas.Dataframe;

    return:
    fact_venda -- pandas.Dataframe;
    """
    columns_names = {
        "nfc": "NU_NFC",
        "qtd_produto": "QTD_PRODUTO",
        "SK_DATA": "SK_DT_VENDA"
    }

    columns_select = [
        "SK_FORMA_PAGAMENTO",
        "SK_CLIENTE",
        "SK_FUNCIONARIO",
        "SK_LOJA",
        "SK_DT_VENDA",
        "SK_PRODUTO",
        "NU_NFC",
        "QTD_PRODUTO",
        "VL_PRECO_CUSTO",
        "VL_PERCENTUAL_LUCRO"
    ]

    fact_venda = (
        stg_venda.
            rename(columns=columns_names).
            filter(columns_select)
    )

    return fact_venda


def treat_missing_data(fact_venda):
    fact_venda = (
        pd.DataFrame([
            [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1],
            [-2, -2, -2, -2, -2, -2, -2, -2, -2, -2],
            [-3, -3, -3, -3, -3, -3, -3, -3, -3, -3]
        ], columns=fact_venda.columns).append(fact_venda)
    )

    return fact_venda


def load_fact_venda(fact_venda, conn, action):
    """
    Faz a carga da fato venda no DW.

    parâmetros:
    fact_venda -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    data_type = {
        "SK_FORMA_PAGAMENTO": Integer(),
        "SK_CLIENTE": Integer(),
        "SK_FUNCIONARIO": Integer(),
        "SK_LOJA": Integer(),
        "SK_DT_VENDA": Integer(),
        "SK_PRODUTO": Integer(),
        "NU_NFC": String(),
        "QTD_PRODUTO": Integer(),
        "VL_PRECO_CUSTO": Float(),
        "VL_PERCENTUAL_LUCRO": Float()
    }

    (
        fact_venda.
            to_sql(
            con=conn,
            name='F_VENDA',
            schema="DW",
            if_exists=action,
            chunksize=100,
            index=False,
            dtype=data_type
        )
    )


def run_fact_venda(conn):
    """
    Executa o pipeline da fato venda.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    fact_venda = verify_fact_exists(conn)
    if fact_venda is None:
        (
            extract_new_venda(conn).
                pipe(treat_fact_venda).
                pipe(treat_missing_data).
                pipe(load_fact_venda, conn=conn, action='replace')
        )
    else:
        (
            extract_new_values(conn).
                pipe(treat_fact_venda).
                pipe(load_fact_venda, conn=conn, action='append')
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
    run_fact_venda(conn_dw)
    print(f'exec time = {t.time() - start}')
