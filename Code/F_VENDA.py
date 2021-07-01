import time as t
import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database, merge_input
import DW_TOOLS as dwt


def extract_fact_venda(conn):
    stage_venda = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_VENDA',
        columns=['id_venda', 'id_pagamento', 'id_cliente',
                 'id_func', 'id_loja', 'nfc', 'data_venda']
    ).assign(
        data_venda=lambda x: x.data_venda.apply(
            lambda y: y[:13]
        )
    )

    stage_item_venda = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_ITEM_VENDA'
    )

    dim_forma_pagamento = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_FORMA_PAGAMENTO',
        columns=["SK_FORMA_PAGAMENTO", "CD_FORMA_PAGAMENTO"]
    )

    dim_endereco = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_ENDERECO',
        columns=["SK_ENDERECO", "CD_ENDERECO"]
    )

    stage_cliente = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_CLIENTE',
        columns=['id_cliente', 'id_endereco']
    )

    dim_cliente = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_CLIENTE',
        columns=["SK_CLIENTE", "CD_CLIENTE"]
    ).merge(
        right=stage_cliente,
        left_on='CD_CLIENTE',
        right_on='id_cliente',
        how='left'
    ).merge(
        right=dim_endereco,
        left_on='id_endereco',
        right_on='CD_ENDERECO',
        how='left'
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
        columns=["SK_LOJA", "CD_LOJA"]
    )

    dim_data = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_DATA',
        columns=['SK_DATA', 'DT_REFERENCIA']
    ).assign(
        DT_REFERENCIA=lambda x: x.DT_REFERENCIA.apply(
            lambda y: y[:13]
        )
    )

    dim_produto = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_PRODUTO',
        columns=["SK_PRODUTO", "CD_PRODUTO", "CD_CATEGORIA",
                 "VL_PRECO_CUSTO", "VL_PERCENTUAL_LUCRO"]
    )

    fact_venda = (
        stage_venda.
            pipe(merge_input,
                 right=dim_data,
                 left_on="data_venda",
                 right_on="DT_REFERENCIA",
                 suff=['_16', '_17'],
                 surrogate_key='SK_DATA').
            pipe(merge_input,
                 right=dim_forma_pagamento,
                 left_on="id_pagamento",
                 right_on="CD_FORMA_PAGAMENTO",
                 suff=["_01", "_02"],
                 surrogate_key="SK_FORMA_PAGAMENTO").
            pipe(merge_input,
                 right=dim_cliente,
                 left_on="id_cliente",
                 right_on="CD_CLIENTE",
                 suff=["_03", "_04"],
                 surrogate_key="SK_CLIENTE").
            pipe(merge_input,
                 right=dim_funcionario,
                 left_on="id_func",
                 right_on="CD_FUNCIONARIO",
                 suff=["_05", "_06"],
                 surrogate_key="SK_FUNCIONARIO").
            pipe(merge_input,
                 right=dim_loja,
                 left_on="id_loja",
                 right_on="CD_LOJA",
                 suff=["_07", "_08"],
                 surrogate_key="SK_LOJA").
            pipe(pd.merge,
                 right=stage_item_venda,
                 left_on="id_venda",
                 right_on="id_venda",
                 suffixes=["_11", "_12"]).
            pipe(merge_input,
                 right=dim_produto,
                 left_on="id_produto",
                 right_on="CD_PRODUTO",
                 suff=["_13", "_14"],
                 surrogate_key="SK_PRODUTO")
    )

    return fact_venda


def treat_fact_venda(fact_tbl):
    columns_names = {
        "nfc": "NU_NFC",
        "qtd_produto": "QTD_PRODUTO",
        "SK_DATA": "SK_DT_VENDA",
        "SK_ENDERECO": "SK_ENDERECO_CLIENTE"
    }

    columns_select = [
        "SK_FORMA_PAGAMENTO",
        "SK_CLIENTE",
        "SK_FUNCIONARIO",
        "SK_LOJA",
        "SK_DT_VENDA",
        "SK_PRODUTO",
        "SK_ENDERECO_CLIENTE",
        "SK_CATEGORIA",
        "NU_NFC",
        "QTD_PRODUTO",
        "VL_PRECO_CUSTO",
        "VL_PERCENTUAL_LUCRO"
    ]

    fact_venda = (
        fact_tbl.
            rename(columns=columns_names).
            assign(
            SK_CLIENTE=lambda x: x.SK_CLIENTE.astype('int64'),
            SK_ENDERECO_CLIENTE=lambda x: x.SK_ENDERECO_CLIENTE.apply(
                lambda y: -3 if pd.isna(y) else y)).
            filter(columns_select)
    )

    fact_venda = (
        pd.DataFrame([
            [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1, -1],
            [-2, -2, -2, -2, -2, -2, -2, -2, -2, -2, -2],
            [-3, -3, -3, -3, -3, -3, -3, -3, -3, -3, -3]
        ], columns=fact_venda.columns).append(fact_venda)
    )

    return fact_venda


def load_fact_venda(fact_venda, conn):
    fact_venda.to_sql(
        con=conn,
        name='F_VENDA',
        schema="DW",
        if_exists='replace',
        chunksize=100,
        index=False
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
