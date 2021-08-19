import time as t
import pandas as pd
from pandasql import sqldf
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import String, Integer
import Code.DW_TOOLS as dwt
from Code.CONEXAO import create_connection_postgre


def extract_dim_cliente(conn):
    """
    Extrai os registros da stages relacionadas ao cliente

    :parameter:
        conn -- sqlalchemy.engine;

    :return:
        stg_cliente_endereco -- pandas.Dataframe;
    """

    stg_cliente = (
        dwt.read_table(
            conn=conn,
            schema="stage",
            table_name="stg_cliente",
            columns=[
                'id_cliente',
                'nome',
                'cpf',
                'tel',
                'id_endereco']).
        assign(
            tel=lambda x: x.tel.apply(
                lambda y: y[0:8] + y[-5:]
            )
        )
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

    tbl_cliente = (
        pd.merge(
            left=stg_cliente,
            right=stg_endereco,
            left_on='id_endereco',
            right_on='id_endereco',
            how='left'
        )
    )

    if dwt.verify_table_exists(conn=conn, table='d_cliente_v2', schema='dw'):
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
                    stg.rua,
                    CASE 
                        WHEN 
                            stg.nome != dim.no_cliente
                            OR stg.cpf != dim.nu_cpf
                            OR stg.tel != dim.nu_telefone
                            OR stg.id_endereco != dim.cd_endereco_cliente
                        THEN 'update'
                        WHEN dim.cd_cliente IS NULL
                        THEN 'insert'
                        ELSE 'none' 
                    END AS fl_insert
                FROM stg_cliente stg 
                LEFT JOIN dw.d_cliente_v2 dim 
                ON stg.id_cliente = dim.cd_cliente;
            """
        tbl_cliente = sqldf(query, {'stg_cliente': tbl_cliente}, conn.url)
    else:
        tbl_cliente = (
            tbl_cliente.assign(
                fl_insert='insert'
            )
        )

    return tbl_cliente


def treat_dim_cliente(tbl_cliente, conn):
    """
    Faz o tratamento dos registros encontrados na stage

    :parameter:
        conn -- sqlalchemy.engine;
    :parameter:
       tbl_cliente -- pandas.Dataframe;

    :return:
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
        "id_endereco",
        "estado",
        "cidade",
        "bairro",
        "rua",
        'fl_insert'
    ]

    dim_cliente = (
        tbl_cliente.
        filter(select_columns).
        rename(columns=columns_name).
        assign(
            nu_telefone=lambda x: x.nu_telefone.apply(
                lambda y: y[0:8] + y[-5:]
            )
        )
    )

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_cliente_v2'):
        size = dwt.find_max_sk(
            conn=conn,
            schema='dw',
            table='d_cliente',
            sk_name='sk_cliente'
        )

        dim_cliente.insert(0, 'sk_cliente', range(size, size + len(dim_cliente)))
    else:

        del dim_cliente['fl_insert']
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


def update_values(dim_cliente, conn):
    """
    Faz update dos dados na dim_cliente
    :parameter:
        dim_cliente -- pandas.Dataframe
    :parameter:
        conn -- sqlalchemy.engine;
    """
    Session = sqla.orm.sessionmaker(conn)
    session = Session()
    try:
        metadata = sqla.MetaData(bind=conn)
        datatable = sqla.Table('d_cliente_v2', metadata, schema='dw', autoload=True)

        for row in dim_cliente.iterrows():

            update = (
                sqla.sql.update(datatable).values(
                    {'no_cliente': row[1]['no_cliente'],
                     'nu_cpf': row[1]['nu_cpf'],
                     'nu_telefone': row[1]['nu_telefone'],
                     'cd_endereco_cliente': row[1]['cd_endereco_cliente'],
                     'no_estado': row[1]['no_estado'],
                     'no_bairro': row[1]['no_estado'],
                     'ds_rua': row[1]['ds_rua']}).
                where(
                     datatable.c.cd_cliente == row[1]['cd_cliente']
                )
            )
            session.execute(update)
            session.flush()
            session.commit()
    finally:
        session.close()


def load_dim_cliente(dim_cliente, conn):
    """
    Faz a carga da dimensão cliente no DW.

    :parameter:
        dim_cliente -- pandas.Dataframe;
    :parameter:
        conn -- sqlalchemy.engine;

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

    if 'fl_insert' in dim_cliente.columns:
        tbl_update = dim_cliente.query('fl_insert == "update"')
        update_values(tbl_update, conn)
        dim_cliente = dim_cliente.query('fl_insert == "insert"')
        del dim_cliente['fl_insert']

    (
        dim_cliente.
        astype('string').
        to_sql(
            con=conn,
            name='d_cliente_v2',
            schema='dw',
            if_exists='append',
            index=False,
            chunksize=100,
            dtype=data_type
        )
    )


def run_dim_cliente(conn):
    """
    Executa o pipeline da dimensão cliente.

    :parameter:
        conn -- sqlalchemy.engine;
    """

    if dwt.verify_table_exists(conn=conn, schema='stage', table='stg_cliente'):
        tbl_cliente = extract_dim_cliente(conn)

        if tbl_cliente.shape[0] != 0:
            (
                treat_dim_cliente(tbl_cliente=tbl_cliente, conn=conn).
                pipe(load_dim_cliente, conn=conn)
            )


if __name__ == '__main__':
    conn = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )
    start = t.time()
    run_dim_cliente(conn)
    print(f'exec time = {t.time() - start}')
