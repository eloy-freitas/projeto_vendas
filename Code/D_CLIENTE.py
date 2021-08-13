import time as t
import pandas as pd
from pandasql import sqldf
from sqlalchemy.types import String, Integer
import Code.DW_TOOLS as dwt
from Code.CONEXAO import create_connection_postgre


def extract_dim_cliente(conn):
    """
    Extrai os registros da stg_cliente

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_cliente_endereco -- dataframe dos registros
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

    if dwt.verify_table_exists(conn=conn, table='d_cliente', schema='dw'):
        query = """
                SELECT 
                    stg.id_cliente, 
                    stg.nome, 
                    stg.cpf, 
                    stg.tel, 
                    stg.id_endereco, 
                    stg.estado, 
                    stg.cidade, 
                    stg.bairro, 
                    stg.rua 
                FROM stg_cliente stg 
                LEFT JOIN dw.d_cliente dim 
                ON stg.id_cliente = dim.cd_cliente 
                WHERE dim.cd_cliente IS NULL
            """
        stg_cliente_endereco = sqldf(query, {'stg_cliente': stg_cliente_endereco}, conn.url)

    return stg_cliente_endereco


def treat_dim_cliente(new_values, conn):
    """
    Faz o tratamento dos registros encontrados na stage

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

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_cliente'):
        size = dwt.find_max_sk(conn=conn, schema='dw', table='d_cliente')
        dim_cliente.insert(0, 'sk_cliente', range(size, size + len(dim_cliente)))
    else:
        dim_cliente.insert(0, 'sk_cliente', range(1, 1 + len(dim_cliente)))

        dim_cliente = (
            pd.DataFrame([
                [-1, -1, "Não informado", "Não informado", "Não informado", -1,
                 "Não informado", "Não informado", "Não informado", "Não informado"],
                [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", -2,
                 "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável"],
                [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", -3,
                 "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido"]
            ], columns=dim_cliente.columns).append(dim_cliente)
        )
    return dim_cliente


def load_dim_cliente(dim_cliente, conn, action):
    """
    Faz a carga da dimensão cliente no DW.

    parâmetros:
    dim_cliente -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    action -- ação que dever ser feita (raplace ou append)
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

    if not dwt.verify_table_exists(conn=conn, schema='dw', table='d_cliente'):
        (
            extract_dim_cliente(conn).
            pipe(treat_dim_cliente, conn=conn).
            pipe(load_dim_cliente, conn=conn, action='replace')
        )
    else:
        (
            extract_dim_cliente(conn).
            pipe(treat_dim_cliente, conn=conn).
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

