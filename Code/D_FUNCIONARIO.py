import pandas as pd
import time as t
from pandasql import sqldf
from sqlalchemy.types import Date, String, Integer
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt


def extract_dim_funcionario(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão funcionario

    :parameter:
        conn -- sqlalchemy.engine;

    :return:
        stg_funcionario -- pandas.Dataframe;
    """
    stg_funcionario = dwt.read_table(
        conn=conn,
        schema='stage',
        table_name='stg_funcionario',
        columns=[
            'id_funcionario',
            'nome',
            'cpf',
            'tel',
            'data_nascimento'
        ]
    )

    if dwt.verify_table_exists(conn=conn, table='d_funcionario', schema='dw'):
        query = """
                SELECT 
                    stg.id_funcionario, 
                    stg.nome, 
                    stg.cpf, 
                    stg.tel, 
                    stg.data_nascimento 
                FROM stg_funcionario stg 
                LEFT JOIN dw.d_funcionario dim 
                ON stg.id_funcionario = dim.cd_funcionario 
                WHERE dim.cd_funcionario IS NULL
            """
        stg_funcionario = sqldf(query, {'stg_funcionario': stg_funcionario}, conn.url)

    return stg_funcionario


def treat_dim_funcionario(stg_funcionario, conn):
    """
    Faz o tratamento dos novos registros encontrados na stage

    :parameter:
        conn -- sqlalchemy.engine;
    :parameter
        stg_funcionario -- pandas.Dataframe;

    :return:
        dim_funcionario -- pandas.Dataframe;
    """
    columns_names = {
        "id_funcionario": "cd_funcionario",
        "nome": "no_funcionario",
        "cpf": "nu_cpf",
        "tel": "nu_telefone",
        "data_nascimento": "dt_nascimento"
    }

    select_columns = [
        "id_funcionario",
        "nome",
        "cpf",
        "tel",
        "data_nascimento"
    ]

    dim_funcionario = (
        stg_funcionario.
        filter(select_columns).
        rename(columns=columns_names).
        assign(
            dt_nascimento=lambda x: x.dt_nascimento.astype('datetime64'))
    )

    if dwt.verify_table_exists(conn=conn, table='d_funcionario', schema='dw'):
        size = dwt.find_max_sk(
            conn=conn,
            schema='dw',
            table='d_funcionario',
            sk_name='sk_funcionario'
        )

        dim_funcionario.insert(0, 'sk_funcionario', range(size, size + len(dim_funcionario)))
    else:

        defaut_date = pd.to_datetime("1900-01-01", format='%Y-%m-%d')

        dim_funcionario.insert(0, 'sk_funcionario', range(1, 1 + len(dim_funcionario)))

        dim_funcionario = (
            pd.DataFrame([
                [-1, -1, "Não informado", -1, -1, defaut_date],
                [-2, -2, "Não aplicável", -2, -2, defaut_date],
                [-3, -3, "Desconhecido", -3, -3, defaut_date]
            ], columns=dim_funcionario.columns).append(dim_funcionario)
        )

    return dim_funcionario


def load_dim_funcionario(dim_funcionario, conn):
    """
    Faz a carga da dimensão funcionario no DW.

    :parameter:
        dim_funcionario -- pandas.Dataframe;
    :parameter:
        conn -- sqlalchemy.engine;
    """
    data_types = {
        "sk_funcionario": Integer(),
        "cd_funcionario": Integer(),
        "no_funcionario": String(),
        "nu_cpf": String(),
        "nu_telefone": String(),
        "dt_nascimento": Date()
    }

    (
        dim_funcionario.
        astype('string').
        to_sql(
            con=conn,
            name='d_funcionario',
            schema='dw',
            if_exists='append',
            index=False,
            chunksize=100,
            dtype=data_types
        )

    )


def run_dim_funcionario(conn):
    """
    Executa o pipeline da dimensão funcionario.

    :parameter:
        conn -- sqlalchemy.engine;
    """
    if dwt.verify_table_exists(conn=conn, schema='stage', table='stg_funcionario'):
        tbl_funcionario = extract_dim_funcionario(conn)

        if tbl_funcionario.shape[0] != 0:
            (
                treat_dim_funcionario(stg_funcionario=tbl_funcionario, conn=conn).
                pipe(load_dim_funcionario, conn=conn)
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
    run_dim_funcionario(conn_dw)
    print(f'exec time = {t.time() - start}')
