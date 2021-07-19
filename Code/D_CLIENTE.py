import pandas as pd
import time as t
from sqlalchemy import Integer
from sqlalchemy.types import String
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt


def extract_dim_cliente(conn):
    stg_cliente = dwt.read_table(
        conn=conn,
        schema="STAGE",
        table_name="STAGE_CLIENTE",
    )

    stg_endereco = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_ENDERECO'
    )

    dim_cliente = (
        pd.merge(
            left=stg_cliente,
            right=stg_endereco,
            left_on='id_endereco',
            right_on='id_endereco',
            how='inner'
        )
    )
    
    return dim_cliente


def treat_dim_cliente(dim_cliente):
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
        dim_cliente.
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

    start = t.time()
    run_dim_cliente(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
