import time as t
import pandas as pd
from pandasql import sqldf
from sqlalchemy.types import String, Integer
import DW_TOOLS as dwt
from CONEXAO import create_connection_postgre


def extract_stg_cliente(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão cliente

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_cliente_endereco -- pandas.Dataframe;
    """
    stg_cliente = dwt.read_table(
        conn=conn,
        schema="stage",
        table_name="stg_cliente",
        columns=[
            'id_cliente',
            'nome',
            'cpf',
            'tel',
            'id_endereco'
        ]
    )

    stg_endereco = dwt.read_table(
        conn=conn,
        schema='stage',
        table_name='stg_endereco',
        columns=[
            'id_endereco',
            'estado',
            'cidade',
            'bairro',
            'rua'
        ]
    )

    stg_cliente_endereco = (
        pd.merge(
            left=stg_cliente,
            right=stg_endereco,
            left_on='id_endereco',
            right_on='id_endereco',
            how='left'
        )
    )

    return stg_cliente_endereco


def extract_dim_cliente(conn):
    """
    Extrai os registros da dim_cliente do DW

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_cliente -- dataframe da dim_cliente
    """
    try:
        dim_cliente = dwt.read_table(
            conn=conn,
            schema="dw",
            table_name="d_cliente",
            columns=[
                'sk_cliente',
                'cd_cliente',
                'no_cliente',
                'nu_cpf',
                'nu_telefone',
                'cd_endereco_cliente',
                'no_estado',
                'no_bairro',
                'ds_rua'
            ]
        )
        return dim_cliente
    except:
        return None


def extract_new_cliente(conn):
    """
    Extrai novos registros na stage

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    new_values -- dataframe com as atualizações
    """
    dim_cliente = extract_dim_cliente(conn)

    stg_cliente = (
        extract_stg_cliente(conn).assign(
            tel=lambda x: x.tel.apply(
                lambda y: y[0:8] + y[-5:]
            )
        )
    )

    new_clientes = (
        sqldf('\
            SELECT * FROM \
            stg_cliente stg \
            LEFT JOIN dim_cliente dim \
            ON stg.id_cliente = dim.cd_cliente \
            WHERE dim.cd_cliente IS NULL')
    )

    new_values = (
        new_clientes.assign(
            df_size=dim_cliente['sk_cliente'].max() + 1
        )
    )

    return new_values


def treat_new_cliente(new_values):
    """
    Faz o tratamento dos novos registros encontrados na stage

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;
    new_values -- novos registros ou registros atualizados no formato pandas.Dataframe;

    return:
    trated_values -- registros atualizados no formato pandas.Dataframe;
    """
    columns_name = {
        "id_cliente": "cd_cliente",
        "nome": "no_cliente",
        "cpf": "nu_cpf",
        "tel": "nu_telefone",
        "id_endereco": "cd_endereco_cliente",
        "estado": "no_estado",
        "cidade": "no_cidade",
        "bairro": "no_bairro",
        "rua": "ds_rua"
    }

    select_columns = [
        "id_cliente",
        "nome",
        "cpf",
        "tel",
        "id_endereco",
        "estado",
        "cidade",
        "bairro",
        "rua"
    ]

    dim_cliente = (
        new_values.
        filter(select_columns).
        rename(columns=columns_name).
        assign(
            nu_telefone=lambda x: x.nu_telefone.apply(
                lambda y: y[0:8] + y[-5:]
            )
        )
    )

    size = new_values['df_size'].max()
    dim_cliente.insert(0, 'sk_cliente', range(size, size + len(dim_cliente)))

    return dim_cliente


def treat_dim_cliente(stg_cliente_endereco):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_cliente_endereco -- pandas.Dataframe;

    return:
    dim_cliente -- pandas.Dataframe;
    """

    columns_name = {
        "id_cliente": "cd_cliente",
        "nome": "no_cliente",
        "cpf": "nu_cpf",
        "tel": "nu_telefone",
        "id_endereco": "cd_endereco_cliente",
        "estado": "no_estado",
        "cidade": "no_cidade",
        "bairro": "no_bairro",
        "rua": "ds_rua"
    }

    select_columns = [
        "id_cliente",
        "nome",
        "cpf",
        "tel",
        'id_endereco',
        "estado",
        "cidade",
        "bairro",
        "rua"
    ]

    dim_cliente = (
        stg_cliente_endereco.
        filter(select_columns).
        rename(columns=columns_name).
        assign(
            nu_telefone=lambda x: x.nu_telefone.apply(
                lambda y: y[0:8] + y[-5:])
        )
    )

    dim_cliente.insert(0, 'sk_cliente', range(1, 1 + len(dim_cliente)))

    dim_cliente = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", "Não informado", -1, "Não informado", "Não informado", "Não informado", "Não informado"],
            [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável"],
            [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido"]
        ], columns=dim_cliente.columns).append(dim_cliente)
    )

    return dim_cliente


def load_dim_cliente(dim_cliente, conn, action):
    """
    Faz a carga da dimensão cliente no DW.

    parâmetros:
    dim_cliente -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    data_type = {
        "sk_cliente": Integer(),
        "cd_cliente": Integer(),
        "no_cliente": String(),
        "nu_cpf": String(),
        "nu_telefone": String(),
        "cd_endereco_cliente": Integer(),
        "no_estado": String(),
        "no_cidade": String(),
        "no_bairro": String(),
        "ds_rua": String()
    }
    (
        dim_cliente.
            astype('string').
            to_sql(
            con=conn,
            name='d_cliente',
            schema='dw',
            if_exists=action,
            index=False,
            chunksize=100,
            dtype=data_type
        )
    )


def run_dim_cliente(conn):
    """
    Executa o pipeline da dimensão cliente.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    dim_cliente = extract_dim_cliente(conn)
    if dim_cliente is None:
        (
            extract_stg_cliente(conn).
                pipe(treat_dim_cliente).
                pipe(load_dim_cliente, conn=conn, action='replace')
        )
    else:
        (
            extract_new_cliente(conn).
                pipe(treat_new_cliente).
                pipe(load_dim_cliente, conn=conn, action='append')
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
    run_dim_cliente(conn_dw)
    print(f'exec time = {t.time() - start}')

