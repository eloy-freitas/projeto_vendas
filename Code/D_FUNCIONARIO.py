import pandas as pd
import time as t
from pandasql import sqldf
from sqlalchemy.types import Date, String, Integer
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt


def extract_stg_funcionario(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão funcionario

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
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

    return stg_funcionario


def extract_dim_funcionario(conn):
    """
    Extrai os registros da dim_funcionario do DW

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_funcionario -- dataframe da dim_funcionario
    """
    try:
        dim_funcionario = dwt.read_table(
            conn=conn,
            schema='dw',
            table_name='d_funcionario',
            columns=[
                'sk_funcionario',
                'cd_funcionario',
                'no_funcionario',
                'nu_cpf',
                'nu_telefone',
                'dt_nascimento'
            ]
        )

        return dim_funcionario
    except:
        return None


def extract_new_funcionario(conn):
    """
    Extrai novos registros na stage

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    new_values -- dataframe com as atualizações
    """
    dim_funcionario = extract_dim_funcionario(conn)

    stg_funcionario = extract_stg_funcionario(conn)

    new_funcionarios = (
        sqldf('\
                SELECT * FROM \
                stg_funcionario stg \
                LEFT JOIN dim_funcionario dim \
                ON stg.id_funcionario = dim.cd_funcionario \
                WHERE dim.cd_funcionario IS NULL')
    )

    new_values = (
        new_funcionarios.assign(
            df_size=dim_funcionario['sk_funcionario'].max() + 1
        )
    )

    return new_values


def treat_new_funcionario(new_values):
    """
    Faz o tratamento dos novos registros encontrados na stage

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;
    new_values -- novos registros ou registros atualizados no formato pandas.Dataframe;

    return:
    trated_values -- dataframe dos novos registros tratados;
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
        new_values.
        filter(select_columns).
        rename(columns=columns_names).
        assign(
            dt_nascimento=lambda x: x.dt_nascimento.astype('datetime64'))
    )

    size = new_values['df_size'].max()
    dim_funcionario.insert(0, 'sk_funcionario', range(size, size + len(dim_funcionario)))

    return dim_funcionario


def treat_dim_funcionario(stg_funcionario):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_funcionario -- pandas.Dataframe;

    return:
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
            dt_nascimento=lambda x: x.dt_nascimento.astype('datetime64')
        )
    )

    dim_funcionario.insert(0, 'sk_funcionario', range(1, 1 + len(dim_funcionario)))

    dim_funcionario = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, None],
            [-2, -2, "Não aplicável", -2, -2, None],
            [-3, -3, "Desconhecido", -3, -3, None]
        ], columns=dim_funcionario.columns).append(dim_funcionario)
    )

    return dim_funcionario


def load_dim_funcionario(dim_funcionario, conn, action):
    """
    Faz a carga da dimensão funcionario no DW.

    parâmetros:
    dim_funcionario -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
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
            if_exists=action,
            index=False,
            chunksize=100,
            dtype=data_types
        )

    )


def run_dim_funcionario(conn):
    """
    Executa o pipeline da dimensão funcionario.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    dim_funcionario = extract_dim_funcionario(conn)
    if dim_funcionario is None:
        (
            extract_stg_funcionario(conn).
            pipe(treat_dim_funcionario).
            pipe(load_dim_funcionario, conn=conn, action='replace')
        )
    else:
        (
            extract_new_funcionario(conn).
            pipe(treat_new_funcionario).
            pipe(load_dim_funcionario, conn=conn, action='append')
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
