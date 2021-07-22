import time as t
import pandas as pd
from sqlalchemy.types import Integer, String, Float
from pandasql import sqldf
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt


def extract_fact_venda(conn):
    stage_venda = (dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STG_VENDA',
        columns=['id_venda', 'id_pagamento', 'id_cliente',
                 'id_func', 'id_loja', 'nfc', 'data_venda']).
        assign(
        data_venda=lambda x: pd.to_datetime(
            x.data_venda,
            format='%Y-%m-%d %H:%M:%S').
            dt.strftime('%d-%m-%Y %H:%M:%S')).
        assign(
        data_venda=lambda x: x.data_venda.apply(
            lambda y: y[:13]),
        id_venda=lambda x: x.id_venda.astype('int64'),
        id_pagamento=lambda x: x.id_pagamento.astype('int64'),
        id_cliente=lambda x: x.id_cliente.astype('int64'),
        id_func=lambda x: x.id_func.astype('int64'),
        id_loja=lambda x: x.id_loja.astype('int64')
    ))
    stage_item_venda = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STG_ITEM_VENDA'
    ).assign(
        id_venda=lambda x: x.id_venda.astype('int64')
    )

    dim_forma_pagamento = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_FORMA_PAGAMENTO',
        columns=["SK_FORMA_PAGAMENTO", "CD_FORMA_PAGAMENTO"]
    )

    dim_cliente = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_CLIENTE',
        columns=["SK_CLIENTE", "CD_CLIENTE"]
    )

    dim_funcionario = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_FUNCIONARIO',
        columns=["SK_FUNCIONARIO", "CD_FUNCIONARIO"]
    )

    dim_loja = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_LOJA',
        columns=["SK_LOJA", "CD_LOJA", "DT_INICIO", "DT_FIM"]
    )

    dim_data = (
        dwt.read_table(
            conn=conn,
            schema='DW',
            table_name='D_DATA',
            columns=['SK_DATA', 'DT_REFERENCIA']).
            assign(
            DT_REFERENCIA=lambda x: pd.to_datetime(
                x.DT_REFERENCIA,
                format='%Y-%m-%d %H:%M:%S').
                dt.strftime('%d-%m-%Y %H:%M:%S')).
            assign(
            DT_REFERENCIA=lambda x: x.DT_REFERENCIA.astype(str)).
            assign(
            DT_REFERENCIA=lambda x: x.DT_REFERENCIA.apply(
                lambda y: y[:13])
        )
    )

    dim_produto = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_PRODUTO',
        columns=["SK_PRODUTO", "CD_PRODUTO", "DS_CATEGORIA",
                 "VL_PRECO_CUSTO", "VL_PERCENTUAL_LUCRO",
                 'DT_INICIO', 'DT_FIM']
    )
    fact_venda = (
        stage_venda.
            pipe(pd.merge,
                 right=stage_item_venda,
                 left_on="id_venda",
                 right_on="id_venda",
                 suffixes=["_11", "_12"]).
            pipe(dwt.merge_input,
                 right=dim_data,
                 left_on="data_venda",
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

    # convertendo as datas da dim_produto para inteiro
    produtos = (
        dim_produto.loc[3:].
            assign(
            DT_INICIO=lambda x: pd.to_datetime(
                x.DT_INICIO,
                format='%Y-%m-%d %H:%M:%S').
                dt.strftime('%d-%m-%Y %H:%M:%S'),
            DT_FIM=lambda x: pd.to_datetime(
                x.DT_FIM,
                format='%Y-%m-%d %H:%M:%S').
                dt.strftime('%d-%m-%Y %H:%M:%S')).
            assign(
            DT_INICIO=lambda x: x.DT_INICIO.astype(str),
            DT_FIM=lambda x: x.DT_FIM.astype(str)).
            assign(
            DT_INICIO=lambda x: x.DT_INICIO.apply(
                lambda y: y[:13]),
            DT_FIM=lambda x: x.DT_FIM.apply(
                lambda y: y[:13])).
            pipe(
            dwt.merge_input,
            right=dim_data,
            left_on='DT_INICIO',
            right_on='DT_REFERENCIA',
            suff=['_01', '_02'],
            surrogate_key='SK_DATA').
            pipe(
            dwt.merge_input,
            right=dim_data,
            left_on='DT_FIM',
            right_on='DT_REFERENCIA',
            suff=['_03', '_04'],
            surrogate_key='SK_DATA').
            rename(
            columns={
                'SK_DATA_03': 'SK_DT_INICIO',
                'SK_DATA_04': 'SK_DT_FIM'
            }
        )
    )

    # convertendo as datas da dim_loja para inteiro
    lojas = (
        dim_loja.loc[3:].
            assign(
            DT_INICIO=lambda x: pd.to_datetime(
                x.DT_INICIO,
                format='%Y-%m-%d %H:%M:%S').
                dt.strftime('%d-%m-%Y %H:%M:%S'),
            DT_FIM=lambda x: pd.to_datetime(
                x.DT_FIM,
                format='%Y-%m-%d %H:%M:%S').
                dt.strftime('%d-%m-%Y %H:%M:%S')).
            assign(
            DT_INICIO=lambda x: x.DT_INICIO.astype(str),
            DT_FIM=lambda x: x.DT_FIM.astype(str)).
            assign(
            DT_INICIO=lambda x: x.DT_INICIO.apply(
                lambda y: y[:13]),
            DT_FIM=lambda x: x.DT_FIM.apply(
                lambda y: y[:13])).
            pipe(
            dwt.merge_input,
            right=dim_data,
            left_on='DT_INICIO',
            right_on='DT_REFERENCIA',
            suff=['_01', '_02'],
            surrogate_key='SK_DATA').
            pipe(
            dwt.merge_input,
            right=dim_data,
            left_on='DT_FIM',
            right_on='DT_REFERENCIA',
            suff=['_03', '_04'],
            surrogate_key='SK_DATA').
            rename(
            columns={
                'SK_DATA_03': 'SK_DT_INICIO',
                'SK_DATA_04': 'SK_DT_FIM'
            }
        )
    )

    fact_venda = (
        sqldf(f'SELECT\
            fv.*, p.SK_PRODUTO, p.VL_PRECO_CUSTO,\
            p.VL_PERCENTUAL_LUCRO\
            FROM fact_venda fv\
            LEFT JOIN produtos p on fv.id_produto = p.CD_PRODUTO\
            WHERE p.CD_PRODUTO = fv.id_produto \
                AND p.SK_DT_INICIO <= fv.SK_DATA \
                    AND fv.SK_DATA <= p.SK_DT_FIM;')
    )

    fact_venda = (
        sqldf(f'SELECT\
            fv.*, l.SK_LOJA \
            FROM fact_venda fv\
            LEFT JOIN lojas l on fv.id_loja = l.CD_LOJA\
            WHERE l.CD_LOJA = fv.id_loja \
                AND l.SK_DT_INICIO <= fv.SK_DATA \
                    AND fv.SK_DATA <= l.SK_DT_FIM;')
    )

    return fact_venda


def treat_fact_venda(fact_tbl):
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
        fact_tbl.
            rename(columns=columns_names).
            filter(columns_select)
    )

    fact_venda = (
        pd.DataFrame([
            [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1],
            [-2, -2, -2, -2, -2, -2, -2, -2, -2, -2],
            [-3, -3, -3, -3, -3, -3, -3, -3, -3, -3]
        ], columns=fact_venda.columns).append(fact_venda)
    )

    return fact_venda


def load_fact_venda(fact_venda, conn):
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
            if_exists='replace',
            chunksize=100,
            index=False,
            dtype=data_type
        )
    )


def run_fact_venda(conn):
    (
        extract_fact_venda(conn).
        pipe(treat_fact_venda).
        pipe(load_fact_venda, conn=conn)
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
    run_fact_venda(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
