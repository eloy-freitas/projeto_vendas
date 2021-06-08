import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_produto(conn):
    dim_produto = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_PRODUTO";'
    )

    return dim_produto


def treat_dim_produto(dim_produto):
    columns_names = {
        "id_produto": "CD_PRODUTO",
        "nome_produto": "NO_PRODUTO",
        "cod_barra": "CD_BARRA",
        "preco_custo": "VL_PRECO_CUSTO",
        "percentual_lucro": "VL_PERCENTUAL_LUCRO",
        "data_cadastro": "DT_CADASTRO",
        "ativo": "FL_ATIVO"
    }

    dim_produto = (
        dim_produto.
        rename(columns=columns_names).
        assign(
            VL_PRECO_CUSTO=lambda x: x.VL_PRECO_CUSTO.apply(
                lambda y: float(y.replace(",", "."))),
            VL_PERCENTUAL_LUCRO=lambda x: x.VL_PERCENTUAL_LUCRO.apply(
                lambda y: float(y.replace(",", "."))),
            DT_CADASTRO=lambda x: x.DT_CADASTRO.apply(
                lambda y: y[:10])).
        assign(
            DT_CADASTRO=lambda x: x.DT_CADASTRO.astype("datetime64"),
            NO_PRODUTO=lambda x: x.NO_PRODUTO.astype(str),
            FL_ATIVO=lambda x: x.FL_ATIVO.astype("int64"))
    )

    dim_produto.insert(0, 'SK_PRODUTO', range(1, 1 + len(dim_produto)))

    dim_produto = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, -1, -1, -1],
            [-2, -2, "Não aplicável", -2, -2, -2, -2, -2],
            [-3, -3, "Desconhecido", -3, -3, -3, -3, -3]
        ], columns=dim_produto.columns).append(dim_produto)
    )

    return dim_produto


def load_dim_produto(dim_produto, conn):
    insert_data(
        data=dim_produto,
        connection=conn,
        table_name='D_PRODUTO',
        schema_name='DW',
        action='replace'
    )


def run_dim_produto(conn):
    (
        extract_dim_produto(conn).
        pipe(treat_dim_produto).
        pipe(load_dim_produto, conn=conn)
    )


if __name__ == "__main__":
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    run_dim_produto(conn_dw)
