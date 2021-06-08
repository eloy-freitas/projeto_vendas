import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_cliente(conn):
    dim_cliente = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_CLIENTE";'
    )

    return dim_cliente


def treat_dim_cliente(dim_cliente):
    columns_name = {
        "id_cliente": "CD_CLIENTE",
        "nome": "NO_CLIENTE",
        "cpf": "NU_CPF",
        "tel": "NU_TELEFONE",
        "id_endereco": "CD_ENDERECO_CLIENTE"
    }

    dim_cliente = (
        dim_cliente.
        rename(columns=columns_name).
        assign(
            NU_CPF=lambda x: x.NU_CPF.apply(
                lambda y: y[:3] + y[4:7] + y[8:11] + y[12:]),
            NU_TELEFONE=lambda x: x.NU_TELEFONE.apply(
                lambda y: y[1:3] + y[4:8] + y[-4:]
            )).
        assign(
            CD_CLIENTE=lambda x: x.CD_CLIENTE.astype('int64'),
            CD_ENDERECO_CLIENTE=lambda x: x.CD_ENDERECO_CLIENTE.astype("int64")
        )
    )

    dim_cliente.insert(0, 'SK_CLIENTE', range(1, 1 + len(dim_cliente)))

    dim_cliente = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, -1],
            [-2, -2, "Não aplicável", -2, -2, -2],
            [-3, -3, "Desconhecido", -3, -3, -3]
        ], columns=dim_cliente.columns).append(dim_cliente)
    )

    return dim_cliente


def load_dim_cliente(dim_cliente, conn):
    insert_data(dim_cliente, conn, 'D_CLIENTE', 'DW', 'replace')


def run_dim_cliente(conn):
    (
        extract_dim_cliente(conn).
        pipe(treat_dim_cliente).
        pipe(load_dim_cliente, conn=conn)
    )


if __name__ == "__main__":
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    run_dim_cliente(conn_dw)
