import pandas as pd
import unidecode as uc
import time as t
from sqlalchemy.types import String, DateTime, Float, Integer
from pandasql import sqldf
import DW_TOOLS as dwt
from CONEXAO import create_connection_postgre

pd.set_option('display.max_columns', None)
columns_names = {
    "id_produto": "cd_produto",
    "nome_produto": "no_produto",
    "cod_barra": "cd_barra",
    "preco_custo": "vl_preco_custo",
    "percentual_lucro": "vl_percentual_lucro",
    "data_cadastro": "dt_cadastro",
    "ativo": "fl_ativo"
}

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
    parâmetros:
    nome -- string que representa o nome do produto;
    """
    nome = set(str(uc.unidecode(nome)).split())
    data_set = [len(categorias[x].intersection(nome)) for x in categorias]
    result = pd.Series(data=data_set, index=categorias.keys())
    return 'Desconhecido' if result.max() == 0 else result.idxmax()


def extract_stage_produto(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão produto
    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;
    return:
    stg_produto -- pandas.Dataframe;
    """
    stg_produto = dwt.read_table(
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
            'ativo'],
        where=f'"id_produto" > 0 order by "id_produto";'
    )

    return stg_produto


def extract_dim_produto(conn):
    """
    Extrai os dados da dimensão produto
    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;
    return:
    dim_produto -- pandas.Dataframe;
    """
    try:
        dim_produto = dwt.read_table(
            conn=conn,
            schema='dw',
            table_name='d_produto',
            columns=[
                'sk_produto',
                'cd_produto',
                'no_produto',
                'cd_barra',
                'vl_preco_custo',
                'vl_percentual_lucro',
                'dt_cadastro',
                'fl_ativo',
                'dt_inicio',
                'dt_fim',
                'ds_categoria'],
            where=f'"cd_produto" > 0 \
            and "fl_ativo" = 1\
            order by "cd_produto";'
        )
        return dim_produto
    except:
        return None


def treat_dim_produto(stg_produto):
    """
    Faz o tratamento dos dados extraidos das stages
    parâmetros:
    stg_produto -- pandas.Dataframe;
    return:
    dim_produto -- pandas.Dataframe;
    """
    select_columns = [
        "id_produto",
        "nome_produto",
        "cod_barra",
        "preco_custo",
        "percentual_lucro",
        "data_cadastro",
        "ativo"
    ]
    dim_produto = (
        stg_produto.
        filter(select_columns).
        rename(columns=columns_names).
        assign(
            vl_preco_custo=lambda x: x.vl_preco_custo.apply(
                lambda y: float(y.replace(",", "."))),
            vl_percentual_lucro=lambda x: x.vl_percentual_lucro.apply(
                lambda y: float(y.replace(",", "."))),
            dt_cadastro=lambda x: x.dt_cadastro.apply(
                lambda y: y[:10]),
            dt_inicio=lambda x: pd.to_datetime(x.dt_cadastro),
            dt_fim=None).
        assign(
            ds_categoria=lambda x: x.no_produto.apply(
                lambda y: classificar_produto(y))).
        assign(
            dt_cadastro=lambda x: pd.to_datetime(x.dt_cadastro),
            no_produto=lambda x: x.no_produto.astype(str),
            fl_ativo=lambda x: x.fl_ativo.astype("int64"))
    )

    dim_produto.insert(0, 'sk_produto', range(1, 1 + len(dim_produto)))

    dim_produto = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, -1, None, -1, None, None, "Não informado"],
            [-2, -2, "Não aplicável", -2, -2, -2, None, -2, None, None, "Não aplicável"],
            [-3, -3, "Desconhecido", -3, -3, -3, None, -3, None, None, "Desconhecido"]
        ], columns=dim_produto.columns).append(dim_produto)
    )

    return dim_produto


def extract_new_produto(conn):
    """
    Extrai novos registros e registros atualizados na stage
    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;
    return:
    new_values: dataframe com as atualizações
    """
    df_stage = (
        extract_stage_produto(conn).
        rename(columns=columns_names).
        assign(
            vl_preco_custo=lambda x: x.vl_preco_custo.apply(
                lambda y: float(y.replace(",", "."))),
            vl_percentual_lucro=lambda x: x.vl_percentual_lucro.apply(
                lambda y: float(y.replace(",", "."))),
            dt_cadastro=lambda x: x.dt_cadastro.apply(
                lambda y: y[:10])).
        assign(dt_cadastro=lambda x: x.dt_cadastro.astype("datetime64"))
    )

    df_dw = extract_dim_produto(conn)

    new_records = (
        sqldf("\
            SELECT df_stage.*\
            FROM df_stage\
            LEFT JOIN df_dw \
            ON df_stage.cd_produto = df_dw.cd_produto\
            WHERE df_dw.cd_produto IS NULL").
        assign(
            fl_tipo_update=1
        )
    )

    new_updates = (
        sqldf("\
            SELECT stg.* \
            FROM df_stage stg \
            INNER JOIN df_dw dw \
            ON stg.cd_produto = dw.cd_produto \
            WHERE stg.vl_preco_custo != dw.vl_preco_custo\
            OR stg.vl_percentual_lucro != dw.vl_percentual_lucro").
        assign(
            fl_tipo_update=2
        )
    )

    new_names = (
        sqldf("\
            SELECT stg.* \
            FROM df_stage stg \
            INNER JOIN df_dw dw \
            ON stg.cd_produto = dw.cd_produto \
            WHERE stg.no_produto != dw.no_produto").
        assign(
            fl_tipo_update=3
        )
    )

    new_values = (
        pd.concat([new_records, new_updates, new_names]).assign(
            df_size=df_dw['sk_produto'].max() + 1
        )
    )
    print(new_updates)
    return new_values


def treat_new_produto(new_values, conn):
    """
    Faz o tratamento dos fluxos de execução da SCD produto
    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;
    new_values -- novos registros ou registros atualizados no formato pandas.Dataframe;
    return:
    trated_values -- registros atualizados no formato pandas.Dataframe;
    """
    select_columns = [
        "cd_produto",
        "no_produto",
        "cd_barra",
        "dt_cadastro",
        "vl_preco_custo",
        "vl_percentual_lucro"
    ]

    size = new_values['df_size'].max()
    new_names = new_values.query('fl_tipo_update == 3')

    if len(new_values) > 0:
        for cd in new_names['cd_produto']:
            nome = new_names.query(f"cd_produto == {cd}")["no_produto"].item()
            sql = (
                f'\
                UPDATE "dw"."d_produto"\
                SET "no_produto" = \'{str(nome)}\'\
                WHERE "cd_produto" = {cd} AND "fl_ativo" = 1;'
            )
            conn.execute(sql)

    # extraindo linhas que serão atualizadas
    trated_values = (
        new_values.
        query('fl_tipo_update != 3').
        filter(select_columns).
        assign(
            dt_inicio=lambda x: pd.to_datetime("today"),
            dt_fim=None,
            fl_ativo=lambda x: 1,
            dt_cadastro=lambda x: x.dt_cadastro.astype("datetime64"),
            ds_categoria=lambda x: x.no_produto.apply(
                lambda y: classificar_produto(y))
        )
    )

    trated_values.insert(0, 'sk_produto', range(size, size + len(new_values)))

    # identificando as sks que precisam ser atualizadas
    for cd in trated_values['cd_produto']:
        sql = (
            f'\
                UPDATE "dw"."d_produto"\
                SET "fl_ativo" = {0},\
                "dt_fim" = \'{pd.to_datetime("today")}\'\
                WHERE "cd_produto" = {cd} and "fl_ativo" = 1;'
        )
        conn.execute(sql)

    return trated_values


def load_dim_produto(dim_produto, conn, action):
    """
    Faz a carga da dimensão produto no DW.
    parâmetros:
    dim_produto -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    action -- if_exists (append, replace...)
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

    (
        dim_produto.
        astype('string').
        to_sql(
            con=conn,
            name='d_produto',
            schema='dw',
            if_exists=action,
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )


def run_dim_produto(conn):
    """
    Executa o pipeline da dimensão produto.
    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    dim_produto = extract_dim_produto(conn)
    if dim_produto is None:
        (
            extract_stage_produto(conn).
            pipe(treat_dim_produto).
            pipe(load_dim_produto, conn=conn, action='replace')

        )
    else:
        (
            extract_new_produto(conn).
            pipe(treat_new_produto, conn=conn).
            pipe(load_dim_produto, conn=conn, action='append')
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
    run_dim_produto(conn_dw)
    print(f'exec time = {t.time() - start}')