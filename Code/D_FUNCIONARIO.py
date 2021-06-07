import pandas as pd
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_funcionario(conn):
    dim_funcionario = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_FUNCIONARIO"'
    )

    return dim_funcionario


def treat_dim_funcionario(dim_funcionario):
    columns_names = {
        "id_funcionario": "CD_FUNCIONARIO",
        "nome": "NO_FUNCIONARIO",
        "cpf": "NU_CPF",
        "tel": "NU_TELEFONE",
        "data_nascimento": "DT_NASCIMENTO"
    }

    dim_funcionario = (
        dim_funcionario.
        rename(columns=columns_names).
        assign(
            NU_CPF=lambda x: x.NU_CPF.
            apply(lambda y: y[:3] + y[4:7] + y[8:11] + y[12:]),
            NU_TELEFONE=lambda x: x.NU_TELEFONE.
            apply(lambda y: y[1:3] + y[4:8] + y[-4:]),
            DT_NASCIMENTO=lambda x: x.DT_NASCIMENTO.
            astype('datetime64'))
    )

    dim_funcionario.\
        insert(0, 'SK_FUNCIONARIO', range(1, 1 + len(dim_funcionario)))

    dim_funcionario = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, -1],
            [-2, -2, "Não aplicável", -2, -2, -2],
            [-3, -3, "Desconhecido", -3, -3, -3]
        ], columns=dim_funcionario.columns).append(dim_funcionario)
    )

    return dim_funcionario


def load_dim_funcionario(dim_funcionario, conn):
    insert_data(dim_funcionario, conn, 'D_FUNCIONARIO', 'DW', 'replace')


def run_dim_funcionario(conn):
    (
        extract_dim_funcionario(conn).
        pipe(treat_dim_funcionario).
        pipe(load_dim_funcionario, conn=conn)
    )


if __name__ == "__main__":
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    run_dim_funcionario(conn_dw)
