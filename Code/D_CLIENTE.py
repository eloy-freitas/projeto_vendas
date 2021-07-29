import pandas as pd
from sqlalchemy.types import String, Integer
import DW_TOOLS as dwt


def extract_dim_cliente(conn):
    """
    Função que faz extração dos dados das stages cliente e endereço.
    Em seguida faz merge das informações com base no id_endereco
    e retorna o dataframe resultante
    :param sqlalchemy engine (conn):
    :return pandas.Dataframe (stg_cliente_endereco):
    """
    stg_cliente = dwt.read_table(
        conn=conn,
        schema="STAGE",
        table_name="STG_CLIENTE",
    )

    stg_endereco = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STG_ENDERECO'
    )

    stg_cliente_endereco = (
        pd.merge(
            left=stg_cliente,
            right=stg_endereco,
            left_on='id_endereco',
            right_on='id_endereco',
            how='inner'
        )
    )
    
    return stg_cliente_endereco


def treat_dim_cliente(stg_cliente_endereco):
    """
    Função que recebe o dataframe com os dados extraido das stages
    faz o tratamento e transforma na dimensão cliente (dim_cliente)
    :param pandas.Dataframe (stg_cliente_endereco):
    :return pandas.Dataframe (dim_cliente):
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
            [-1, -1, "Não informado", -1, -1, "Não informado", "Não informado", "Não informado", "Não informado"],
            [-2, -2, "Não aplicável", -2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável"],
            [-3, -3, "Desconhecido", -3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido"]
        ], columns=dim_cliente.columns).append(dim_cliente)
    )
    
    return dim_cliente


def load_dim_cliente(dim_cliente, conn):
    """
    Função que recebe um dataframe com os dados da dimensão cliente
    e faz a carga no Data Warehouse
    :param pandas.Dataframe (dim_cliente):
    :param sqlalchemy engine (conn):
    """
    data_type = {
        "SK_CLIENTE": Integer(),
        "CD_CLIENTE": Integer(),
        "NO_CLIENTE": String(),
        "NU_CPF": String(),
        "NU_TELEFONE": String(),
        "CD_ENDERECO_CLIENTE": String(),
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
    Função que executa o pipeline da dimensão cliente consistindo em:
    extração dos dados das stages, tratamento e carregamento no DW
    :param sqlalchemy engine (conn):
    """
    (
        extract_dim_cliente(conn).
            pipe(treat_dim_cliente).
            pipe(load_dim_cliente, conn=conn)
    )

