import pandas as pd
import unidecode as uc
import time as t
import sqlalchemy as sqla
from sqlalchemy.orm import sessionmaker
from sqlalchemy.types import String, DateTime, Float, Integer
from pandasql import sqldf
import Code.DW_TOOLS as dwt
from Code.CONEXAO import create_connection_postgre

pd.set_option('display.max_columns', None)

categorias = {'cafe da manhã': {"CAFE", "ACHOCOLATADO", "CEREAIS", "PAO",
                                "ACUCAR", "ADOCANTE", "BISCOITO",
                                "GELEIA", "IOGURTE", "IORGUTE", "FANDANGOS"},
              'mercearia': {"ARROZ", "FEIJAO", "FARINHA", "TRIGO",
                            "AMIDO", "MILHO", "FERMENTO", "MACARRAO",
                            "MOLHO", "TOMATE", "AZEITE", "OLEO", "SOJA",
                            "OVOS", "TEMPERO", "SAL", "AVEIA",
                            "EXTRATO", "ACUCAR", "SPAGHETTI"
                                                 "SAZON", "CARNE", "SABOR", "FRANGO"
                            },
              'carnes': {"BIFE", "FILE", "BOI", "FRANGO", "PEIXE", "CARNE", "MOIDA",
                         "SALSICHA", "LINGUICA"},
              'bebidas': {"KI", "SUCO", "CERVEJA", "REFRIGERANTE", "VINHO"},
              'higiene': {"SABONETE", "CREME", "DENTAL", "SHAMPOO",
                          "CONDICIONADOR", "ABSORVENTE", "PAPEL", "HIGIENICO",
                          "FRALDA"},
              'hortifruti': {"ALFACE", "CEBOLA", "ALHO", "TOMATE",
                             "LIMAO", "BANANA", "MACA", "BATATA",
                             "DOCE", "INGLESA"},
              'frios': {"LEITE", "PRESUNTO", "QUEIJO", "REQUEIJAO",
                        "MANTEIGA", "CREME"},
              'limpeza': {"AGUA", "SANITARIA", "SABAO", "PO", "PALHA", "AÇO",
                          "AMACIANTE", "DETERGENTE", "SACO", "LIXO",
                          "DESINFETANTE", "PAPEL", "TOALHA", "PAPEL", "HIGIENICO"}
              }


def classificar_produto(nome):
    """
    Classifica o produto baseado nas palavras chaves no nome

    :parameter:
        nome -- string;
    :return:
        result.idxmax() -- coluna do dataframe com maior correspondência
    """
    nome = set(str(uc.unidecode(nome)).split())
    data_set = [len(categorias[x].intersection(nome)) for x in categorias]
    result = pd.Series(data=data_set, index=categorias.keys())
    return 'Desconhecido' if result.max() == 0 else result.idxmax()


def extract_dim_produto(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão produto

    :parameter:
        conn -- sqlalchemy.engine;
    :return:
        stg_produto -- pandas.Dataframe;
    """
    stg_produto = (
        dwt.read_table(
            conn=conn,
            schema='stage',
            table_name='stg_produto',
            columns=[
                'id_produto',
                'nome_produto',
                'cod_barra',
                'preco_custo',
                'percentual_lucro',
                'data_cadastro',
                'ativo']).
        assign(
            preco_custo=lambda x: x.preco_custo.apply(
                lambda y: float(y.replace(",", "."))),
            percentual_lucro=lambda x: x.percentual_lucro.apply(
                lambda y: float(y.replace(",", "."))),
            data_cadastro=lambda x: x.data_cadastro.apply(
                lambda y: y[:10])).
        assign(
            data_cadastro=lambda x: x.data_cadastro.astype("datetime64")
        )
    )

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_produto'):
        query = """
                    SELECT 
                        stg.id_produto, stg.nome_produto, stg.cod_barra, 
                        stg.preco_custo, stg.percentual_lucro, stg.data_cadastro, 
                        stg.ativo,
                            CASE 
                                WHEN dim.cd_produto IS NULL
                                THEN 'insert'
                                WHEN dim.vl_preco_custo != stg.preco_custo
                                    OR dim.vl_percentual_lucro != stg.percentual_lucro
                                THEN 'insert_update'
                                    WHEN dim.no_produto != stg.nome_produto
                                THEN 'only_update'
                                ELSE 'none' 
                            END AS fl_insert_update
                    FROM stg_produto stg
                    LEFT JOIN dw.d_produto dim 
                    ON stg.id_produto = dim.cd_produto
                    WHERE dim.fl_ativo = 1 or dim.fl_ativo is null;
                """
        tbl_produto = sqldf(query, {'stg_produto': stg_produto}, conn.url)
    else:
        tbl_produto = (
            stg_produto.assign(
                fl_insert_update='insert'
            )
        )

    return tbl_produto


def treat_dim_produto(tbl_produto, conn):
    """
    Faz o tratamento dos dados extraidos das stages
    :parameter:
        tbl_produto -- pandas.Dataframe;
    :parameter:
        conn -- sqlalchemy.engine;
    :return:
        dim_produto -- pandas.Dataframe;
    """
    select_columns = [
        "id_produto",
        "nome_produto",
        "cod_barra",
        "preco_custo",
        "percentual_lucro",
        "data_cadastro",
        "ativo",
        "fl_insert_update"
    ]

    columns_names = {
        "id_produto": "cd_produto",
        "nome_produto": "no_produto",
        "cod_barra": "cd_barra",
        "preco_custo": "vl_preco_custo",
        "percentual_lucro": "vl_percentual_lucro",
        "data_cadastro": "dt_cadastro",
        "ativo": "fl_ativo"
    }

    dim_produto = (
        tbl_produto.
        filter(select_columns).
        rename(columns=columns_names).
        assign(
            dt_inicio=lambda x: pd.to_datetime(x.dt_cadastro),
            dt_fim=None,
            fl_ativo=lambda x: 1,
            ds_categoria=lambda x: x.no_produto.apply(
                lambda y: classificar_produto(y))
        )
    )

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_produto'):
        size = dwt.find_max_sk(
            conn=conn,
            schema='dw',
            table='d_produto',
            sk_name='sk_produto'
        )

        dim_produto.insert(0, 'sk_produto', range(size, size + len(dim_produto)))
    else:
        defaut_date = pd.to_datetime("1900-01-01", format='%Y-%m-%d')

        dim_produto.insert(0, 'sk_produto', range(1, 1 + len(dim_produto)))

        del dim_produto['fl_insert_update']

        dim_produto = (
            pd.DataFrame([
                [-1, -1, "Não informado", -1, -1, -1, defaut_date, -1, defaut_date, None, "Não informado"],
                [-2, -2, "Não aplicável", -2, -2, -2, defaut_date, -2, defaut_date, None, "Não aplicável"],
                [-3, -3, "Desconhecido", -3, -3, -3, defaut_date, -3, defaut_date, None, "Desconhecido"]
            ], columns=dim_produto.columns).append(dim_produto)
        )

    return dim_produto


def update_scd_values(dim_produto, conn):
    """
    Faz update dos dados que vão ser desativados na dimensão produto
    :parameter:
        dim_produto -- pandas.Dataframe
    :parameter:
        conn -- sqlalchemy.engine;
    """
    Session = sessionmaker(conn)
    session = Session()
    try:
        metadata = sqla.MetaData(bind=conn)
        datatable = sqla.Table('d_produto', metadata, schema='dw', autoload=True)
        update = (
            sqla.sql.update(datatable).values(
                {'fl_ativo': 0, 'dt_fim': pd.to_datetime("today", format='%Y-%m-%d')}).
            where(
                sqla.and_(
                    datatable.c.cd_produto.in_(dim_produto.cd_produto), datatable.c.fl_ativo == 1
                )
            )
        )
        session.execute(update)
        session.flush()
        session.commit()
    finally:
        session.close()


def update_names(dim_produto, conn):
    """
    Faz update dos dados que não precisam de um novo registro na dimensão produto
    :parameter
        dim_produto -- pandas.Dataframe
    :parameter:
        conn -- sqlalchemy.engine;
    """
    Session = sessionmaker(conn)
    session = Session()
    try:
        metadata = sqla.MetaData(bind=conn)
        datatable = sqla.Table('d_produto', metadata, schema='dw', autoload=True)
        for name in dim_produto['no_produto']:
            update = (
                sqla.sql.update(datatable).values(
                    {'no_produto': name}).
                where(
                    sqla.and_(
                        datatable.c.cd_produto.in_(dim_produto.cd_produto), datatable.c.fl_ativo == 1
                    )
                )
            )
            session.execute(update)
            session.flush()
            session.commit()
    finally:
        session.close()


def load_dim_produto(dim_produto, conn):
    """
    Faz a carga da dimensão produto no DW.
    :parameter:
        dim_produto -- pandas.Dataframe;
    :parameter:
        conn -- sqlalchemy.engine;
    """
    data_types = {
        "sk_produto": Integer(),
        "cd_produto": Integer(),
        "no_produto": String(),
        "cd_barra": String(),
        "vl_preco_custo": Float(),
        "vl_percentual_lucro": Float(),
        "dt_cadastro": DateTime(),
        "fl_ativo": Integer(),
        "dt_inicio": DateTime(),
        "dt_fim": DateTime(),
        "ds_categoria": String()
    }

    if dwt.verify_table_exists(conn=conn, schema='dw', table='d_produto'):
        names_updates = dim_produto.query('fl_insert_update == "only_update"')
        if names_updates.shape[0] != 0:
            update_names(names_updates, conn)

        insert_updates = dim_produto.query('fl_insert_update == "insert_update"')
        if insert_updates.shape[0] != 0:
            update_scd_values(insert_updates, conn)

    if 'fl_insert_update' in dim_produto.columns:
        dim_produto = dim_produto.query('fl_insert_update == "insert_update" or fl_insert_update == "insert"')
        del dim_produto['fl_insert_update']
    (
        dim_produto.
        astype('string').
        to_sql(
            con=conn,
            name='d_produto',
            schema='dw',
            if_exists='append',
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )


def run_dim_produto(conn):
    """
    Executa o pipeline da dimensão produto.
    :parameter:
        conn -- sqlalchemy.engine;
    """

    dwt.verify_table_exists(conn=conn, schema='stage', table='stg_produto')

    if dwt.verify_table_exists(conn=conn, schema='stage', table='stg_produto'):
        tbl_produto = extract_dim_produto(conn)
        df_produto = (
            tbl_produto.query(
                "fl_insert_update == 'insert'\
                or fl_insert_update == 'insert_update' \
                or fl_insert_update == 'only_update'"
            )
        )
        if df_produto.shape[0] != 0:
            (
                treat_dim_produto(tbl_produto=df_produto, conn=conn).
                pipe(load_dim_produto, conn=conn)
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
    run_dim_produto(conn)
    print(f'exec time = {t.time() - start}')