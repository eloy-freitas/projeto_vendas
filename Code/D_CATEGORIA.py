import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data


def treat_dim_categoria():
    categoria_columns = [
        "Café da manhã",
        "Mercearia",
        "Carnes",
        "Bebidas",
        "Higiene",
        "Laticínios/Frios",
        "Limpeza",
        "Hortifruti"
    ]

    dim_categoria = (
        pd.DataFrame(data=categoria_columns, columns=['DS_CATEGORIA'])
    )

    dim_categoria.insert(
        0,
        'SK_CATEGORIA',
        range(1, 1 + len(dim_categoria))
    )

    dim_categoria = (
        pd.DataFrame([
            [-1, "Não informado"],
            [-2, "Não aplicável"],
            [-3, "Desconhecido"]
        ], columns=dim_categoria.columns).append(dim_categoria)
    )

    return dim_categoria


def load_dim_categoria(dim_categoria, conn):
    insert_data(
        data=dim_categoria,
        connection=conn,
        table_name='D_CATEGORIA',
        schema_name='DW',
        action='replace'
    )


def run_dim_categoria(conn):
    (
        treat_dim_categoria().
        pipe(load_dim_categoria, conn=conn)
    )


if __name__ == "__main__":
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    run_dim_categoria(conn_dw)
