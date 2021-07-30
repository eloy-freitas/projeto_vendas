import time as t
import pandas as pd
from sqlalchemy.types import String, Integer
import DW_TOOLS as dwt
from CONEXAO import create_connection_postgre


def extract_dim_cliente(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão cliente

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_cliente_endereco -- pandas.Dataframe;
    """
    stg_cliente = dwt.read_table(
        conn=conn,
        schema="STAGE",
        table_name="STG_CLIENTE",
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
        schema='STAGE',
        table_name='STG_ENDERECO',
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


def treat_dim_cliente(stg_cliente_endereco):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_cliente_endereco -- pandas.Dataframe;

    return:
    dim_cliente -- pandas.Dataframe;
    """

    columns_name = {
        "id_cliente": "CD_CLIENTE",
        "nome": "NO_CLIENTE",
        "cpf": "NU_CPF",
        "tel": "NU_TELEFONE",
        "id_endereco": "CD_ENDERECO_CLIENTE",
        "estado": "NO_ESTADO",
        "cidade": "NO_CIDADE",
        "bairro": "NO_BAIRRO",
        "rua": "DS_RUA"

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
            NU_TELEFONE=lambda x: x.NU_TELEFONE.apply(
                lambda y: y[0:8] + y[-5:]
            )).
            assign(
            CD_CLIENTE=lambda x: x.CD_CLIENTE.astype('int64')
        )
    )

    dim_cliente.insert(0, 'SK_CLIENTE', range(1, 1 + len(dim_cliente)))

    dim_cliente = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", "Não informado", -1, "Não informado", "Não informado", "Não informado", "Não informado"],
            [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável"],
            [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido"]
        ], columns=dim_cliente.columns).append(dim_cliente)
    )

    return dim_cliente


def load_dim_cliente(dim_cliente, conn):
    """
    Faz a carga da dimensão cliente no DW.

    parâmetros:
    dim_cliente -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    data_type = {
        "SK_CLIENTE": Integer(),
        "CD_CLIENTE": Integer(),
        "NO_CLIENTE": String(),
        "NU_CPF": String(),
        "NU_TELEFONE": String(),
        "CD_ENDERECO_CLIENTE": Integer(),
        "NO_ESTADO": String(),
        "NO_CIDADE": String(),
        "NO_BAIRRO": String(),
        "DS_RUA": String()
    }
    (
        dim_cliente.
            astype('string').
            to_sql(
            con=conn,
            name='D_CLIENTE',
            schema='DW',
            if_exists='replace',
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
    (
        extract_dim_cliente(conn).
            pipe(treat_dim_cliente).
            pipe(load_dim_cliente, conn=conn)
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

