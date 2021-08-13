import datetime
import pandas as pd
import time as t
import sqlalchemy as sqla
from sqlalchemy.orm import Session, sessionmaker
from sqlalchemy.types import DateTime, String, Integer
from pandasql import sqldf
import Code.DW_TOOLS as dwt
from Code.CONEXAO import create_connection_postgre


def extract_dim_loja(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão loja

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_loja_endereco -- pandas.Dataframe;
    """
    stg_loja = dwt.read_table(
        conn=conn,
        schema='stage',
        table_name='stg_loja',
        columns=[
            'id_loja',
            'nome_loja',
            'razao_social',
            'cnpj',
            'telefone',
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

    stg_loja_endereco = (
        stg_loja.pipe(
            pd.merge,
            right=stg_endereco,
            left_on="id_endereco",
            right_on="id_endereco",
            suffixes=["_01", "_02"],
            how='left'
        )
    )

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_loja'):
        query = """
            SELECT 
                stg.id_loja, stg.nome_loja, stg.razao_social, 
                stg.cnpj, stg.telefone, stg.id_endereco, 
                stg.estado, stg.cidade, stg.bairro, stg.rua,
                    CASE 
                        WHEN dim.cd_loja IS NULL
                        THEN 'insert'
                        WHEN dim.cd_endereco_loja != stg.id_endereco
                            OR dim.ds_razao_social != stg.razao_social
                            OR dim.nu_cnpj != stg.cnpj
                        THEN 'insert_update'
                            WHEN dim.no_loja != stg.nome_loja
                        THEN 'only_update'
                        ELSE 'none' 
                    END AS fl_insert_update
            FROM stg_loja stg
            LEFT JOIN dw.d_loja dim 
            ON stg.id_loja = dim.cd_loja
            WHERE dim.fl_ativo = 1
        """
        stg_loja_endereco = sqldf(query, {'stg_loja': stg_loja_endereco}, conn.url)
    else:
        stg_loja_endereco = (
            stg_loja_endereco.assign(
                fl_insert_update='insert'
            )
        )

    return stg_loja_endereco


def treat_dim_loja(stg_loja_endereco, conn):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_loja_endereco -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;

    return:
    dim_loja -- pandas.Dataframe;
    """
    columns_names = {
        "id_loja": "cd_loja",
        "nome_loja": "no_loja",
        "razao_social": "ds_razao_social",
        "cnpj": "nu_cnpj",
        "telefone": "nu_telefone",
        "id_endereco": "cd_endereco_loja",
        "estado": "no_estado",
        "cidade": "no_cidade",
        "bairro": "no_bairro",
        "rua": "ds_rua"
    }

    select_columns = [
        "id_loja",
        "nome_loja",
        "razao_social",
        "cnpj",
        "telefone",
        "id_endereco",
        "estado",
        "cidade",
        "bairro",
        "rua",
        "fl_insert_update"
    ]

    dim_loja = (
        stg_loja_endereco.
        filter(select_columns).
        rename(columns=columns_names).
        assign(
            cd_loja=lambda x: x.cd_loja.astype("int64"),
            fl_ativo=lambda x: 1,
            dt_inicio=pd.to_datetime("today", format='%Y-%m-%d'),
            dt_fim=None
        )
    )

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_loja'):
        size = dwt.find_max_sk(conn=conn, schema='dw', table='d_loja')

        dim_loja.insert(0, 'sk_loja', range(size, size + len(dim_loja)))

    else:
        defaut_date = pd.to_datetime("1900-01-01", format='%Y-%m-%d')

        dim_loja.insert(0, 'sk_loja', range(1, 1 + len(dim_loja)))

        del dim_loja['fl_insert_update']

        dim_loja = (
            pd.DataFrame([
                [-1, -1, "Não informado", "Não informado", "Não informado", "Não informado", -1, "Não informado",
                 "Não informado", "Não informado", "Não informado", -1, defaut_date, None],
                [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável", -2, "Não aplicável",
                 "Não aplicável", "Não aplicável", "Não aplicável", -2, defaut_date, None],
                [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido", -3, "Desconhecido",
                 "Desconhecido", "Desconhecido", "Desconhecido", -3, defaut_date, None]
            ], columns=dim_loja.columns).append(dim_loja)
        )

    return dim_loja


def load_dim_loja(dim_loja, conn, action):
    """
    Faz a carga da dimensão loja no DW.

    parâmetros:
    dim_loja -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    action -- if_exists (append, replace...)
    """
    data_types = {
        "sk_loja": Integer(),
        "cd_loja": Integer(),
        "no_loja": String(),
        "ds_razao_social": String(),
        "nu_cnpj": String(),
        "nu_telefone": String(),
        "cd_endereco_loja": Integer(),
        "no_estado": String(),
        "no_cidade": String(),
        "no_bairro": String,
        "ds_rua": String(),
        "fl_ativo": Integer(),
        "dt_inicio": DateTime(),
        "dt_fim": DateTime()
    }

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_loja'):
        name_updates = dim_loja.query('fl_insert_update == "only_update"')
        if name_updates.shape[0] != 0:
            update_values(name_updates, conn)

        insert_updates = dim_loja.query('fl_insert_update == "insert_update"')
        if insert_updates.shape[0] != 0:
            update_scd_values(insert_updates, conn)

    if 'fl_insert_update' in dim_loja.columns:
        dim_loja = dim_loja.query('fl_insert_update == "insert_update" or fl_insert_update == "insert"')
        del dim_loja['fl_insert_update']

    (
        dim_loja.
        astype('string').
        to_sql(
            con=conn,
            name='d_loja',
            schema='dw',
            if_exists=action,
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )

    return dim_loja


def update_scd_values(dim_loja, conn):
    """
    Faz update dos dados que vão ser desativados na dim_loja
    :param dim_loja: pandas.Dataframe
    :param conn: conexão criada via SqlAlchemy com o servidor do DW;

    """
    Session = sessionmaker(conn)
    session = Session()
    try:
        metadata = sqla.MetaData(bind=conn)
        datatable = sqla.Table('d_loja', metadata, schema='dw', autoload=True)
        update = (
            sqla.sql.update(datatable).values(
                {'fl_ativo': 0, 'dt_fim': pd.to_datetime("today", format='%Y-%m-%d')}).
            where(
                sqla.and_(
                    datatable.c.cd_loja.in_(dim_loja.cd_loja), datatable.c.fl_ativo == 1
                )
            )
        )
        session.execute(update)
        session.flush()
        session.commit()
    finally:
        session.close()


def update_values(dim_loja, conn):
    """
    Faz update dos dados que não precisam de um novo registro na dimensão loja
    :param dim_loja: pandas.Dataframe
    :param conn: conn -- conexão criada via SqlAlchemy com o servidor do DW;

    """
    Session = sessionmaker(conn)
    session = Session()
    try:
        metadata = sqla.MetaData(bind=conn)
        datatable = sqla.Table('d_loja', metadata, schema='dw', autoload=True)
        for name in dim_loja['no_loja']:
            update = (
                sqla.sql.update(datatable).values(
                    {'no_loja': name}).
                where(
                    sqla.and_(
                        datatable.c.cd_loja.in_(dim_loja.cd_loja), datatable.c.fl_ativo == 1
                    )
                )
            )
            session.execute(update)
            session.flush()
            session.commit()
    finally:
        session.close()


def run_dim_loja(conn):
    """
    Executa o pipeline da dimensão loja.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """

    if not dwt.verify_table_exists(conn=conn, schema='dw', table='d_loja'):
        (
            extract_dim_loja(conn=conn).
            pipe(treat_dim_loja, conn=conn).
            pipe(load_dim_loja, conn=conn, action='replace')
        )
    else:
        (
            extract_dim_loja(conn=conn).
            pipe(treat_dim_loja, conn=conn).
            pipe(load_dim_loja, conn=conn, action='append')
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
    run_dim_loja(conn_dw)
    print(f'exec time = {t.time() - start}')
