import time as t
import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database, merge_input


def extract_fact_venda(conn):
    stage_venda = get_data_from_database(
        conn_input=conn,
        sql_query=f'select id_venda, id_pagamento, id_cliente, \
        id_func, id_loja, nfc, data_venda from "STAGES"."STAGE_VENDA";'
    )

    stage_item_venda = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_ITEM_VENDA";'
    )

    dim_forma_pagamento = get_data_from_database(
        conn_input=conn,
        sql_query=f'select "SK_FORMA_PAGAMENTO","CD_FORMA_PAGAMENTO" \
        from "DW"."D_FORMA_PAGAMENTO";'
    )

    dim_cliente = get_data_from_database(
        conn_input=conn,
        sql_query=f'select "SK_CLIENTE", "CD_CLIENTE", "CD_ENDERECO_CLIENTE" \
        from "DW"."D_CLIENTE";'
    )

    dim_funcionario = get_data_from_database(
        conn_input=conn,
        sql_query=f'select "SK_FUNCIONARIO", "CD_FUNCIONARIO"\
        from "DW"."D_FUNCIONARIO";'
    )

    dim_loja = get_data_from_database(
        conn_input=conn,
        sql_query=f'select "SK_LOJA", "CD_LOJA", "CD_ENDERECO_LOJA" \
        from "DW"."D_LOJA";'
    )

    dim_data = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "DW"."D_DATA";'
    )

    dim_produto = get_data_from_database(
        conn_input=conn,
        sql_query=f'select "SK_PRODUTO", "CD_PRODUTO" \
        from "DW"."D_PRODUTO";'
    )

    dim_endereco = get_data_from_database(
        conn_input=conn,
        sql_query=f'select "SK_ENDERECO", "CD_ENDERECO" \
        from "DW"."D_ENDERECO";'
    )

    fact_venda = (
        stage_venda.
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
            pipe(merge_input,
                 right=dim_data,
                 left_on="data_venda",
                 right_on="DT_REFERENCIA",
                 suff=["_09", "_10"],
                 surrogate_key="SK_DATA").
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
                 surrogate_key="SK_PRODUTO").
            pipe(pd.merge,
                 right=dim_endereco,
                 left_on="CD_ENDERECO_LOJA",
                 right_on="CD_ENDERECO",
                 suffixes=["_15", "_16"]).
            pipe(pd.merge,
                 right=dim_endereco,
                 left_on="CD_ENDERECO_CLIENTE",
                 right_on="CD_ENDERECO",
                 suffixes=["_17", "_18"])
    )

    return fact_venda


def treat_fact_venda(fact_tbl):
    columns_names = {
        "nfc": "NU_NFC",
        "qtd_produto": "QTD_PRODUTO",
        "SK_DATA": "SK_DT_VENDA",
        "SK_ENDERECO_17": "SK_ENDERECO_LOJA",
        "SK_ENDERECO_18": "SK_ENDERECO_CLIENTE"
    }

    columns_select = [
        "SK_FORMA_PAGAMENTO",
        "SK_CLIENTE",
        "SK_FUNCIONARIO",
        "SK_LOJA",
        "SK_DT_VENDA",
        "SK_PRODUTO",
        "SK_ENDERECO_LOJA",
        "SK_ENDERECO_CLIENTE",
        "NU_NFC",
        "QTD_PRODUTO"
    ]

    fact_venda = (
        fact_tbl.
            rename(columns=columns_names).
            filter(columns_select)
    )

    print(fact_venda)


def run_fact_venda(conn):
    (
        extract_fact_venda(conn).
            pipe(treat_fact_venda)
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
