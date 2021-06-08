import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_data(conn):
    dim_data = get_data_from_database(
        conn_input=conn,
        sql_query=f'select data_venda from "STAGES"."STAGE_VENDA";'
    )

    return dim_data


def treat_dim_data(dim_data):
    columns_names = {
        "data_venda": "DT_REFERENCIA"
    }

    dim_data = (
        dim_data.
        rename(columns=columns_names)
    )

    dim_data.insert(0, 'SK_DATA', range(1, 1 + len(dim_data)))

    dim_data = (
        pd.DataFrame([
            [-1, "Não informado"],
            [-2, "Não aplicável"],
            [-3, "Desconhecido"]
        ], columns=dim_data.columns).append(dim_data)
    )

    return dim_data


def load_dim_data(dim_data, conn):
    insert_data(
        data=dim_data,
        connection=conn,
        table_name='D_DATA',
        schema_name='DW',
        action='replace'
    )


def run_dim_data(conn):
    (
        extract_dim_data(conn).
        pipe(treat_dim_data).
        pipe(load_dim_data, conn=conn)
    )


if __name__ == "__main__":
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    run_dim_data(conn_dw)
