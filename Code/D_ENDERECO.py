import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_endereco(conn):
    dim_endereco = get_data_from_database(
        conn_input=conn,
        sql_query='select * from "STAGES"."STAGE_ENDERECO"'
    )

    return dim_endereco


def treat_dim_endereco(dim_endereco):
    columns_names = {
        "id_endereco": "CD_ENDERECO",
        "estado": "NO_ESTADO",
        "cidade": "NO_CIDADE",
        "bairro": "NO_BAIRRO",
        "rua": "DS_RUA"
    }

    dim_endereco = (
        dim_endereco.
        rename(columns=columns_names).
        assign(DS_RUA=lambda x: x.DS_RUA.apply(
            lambda y: y.strip()
        ))
    )

    dim_endereco.insert(0,
                        'SK_ENDERECO',
                        range(1, 1 + len(dim_endereco)))

    dim_endereco = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", "Não informado", "Não informado"],
            [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável"],
            [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido"]
        ], columns=dim_endereco.columns).append(dim_endereco)
    )

    return dim_endereco


def load_dim_endereco(dim_endereco, conn):
    insert_data(
        data=dim_endereco,
        connection=conn,
        table_name='D_ENDERECO',
        schema_name='DW',
        action='replace'
    )


def run_dim_endereco(conn):
    (
        extract_dim_endereco(conn).
        pipe(treat_dim_endereco).
        pipe(load_dim_endereco, conn=conn)
    )


if __name__ == "__main__":
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    run_dim_endereco(conn_dw)
