import pandas as pd
import unidecode as uc
import time as t
from sqlalchemy.types import String, DateTime, Float, Integer
from pandasql import sqldf
import DW_TOOLS as dwt
from CONEXAO import create_connection_postgre

pd.set_option('display.max_columns', None)
columns_names = {
    "id_produto": "CD_PRODUTO",
    "nome_produto": "NO_PRODUTO",
    "cod_barra": "CD_BARRA",
    "preco_custo": "VL_PRECO_CUSTO",
    "percentual_lucro": "VL_PERCENTUAL_LUCRO",
    "data_cadastro": "DT_CADASTRO",
    "ativo": "FL_ATIVO"
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
        schema='STAGE',
        table_name='STG_PRODUTO',
        columns=[
            'id_produto',
            'nome_produto',
            'cod_barra',
            'preco_custo',
            'percentual_lucro',
            'data_cadastro',
            'ativo'
        ],
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
            schema='DW',
            table_name='D_PRODUTO',
            columns=[
                'SK_PRODUTO',
                'CD_PRODUTO',
                'NO_PRODUTO',
                'CD_BARRA',
                'VL_PRECO_CUSTO',
                'VL_PERCENTUAL_LUCRO',
                'DT_CADASTRO',
                'FL_ATIVO',
                'DT_INICIO',
                'DT_FIM',
                'DS_CATEGORIA'
            ],
            where=f'"CD_PRODUTO" > 0 \
            and "FL_ATIVO" = 1\
            order by "CD_PRODUTO";'
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
            VL_PRECO_CUSTO=lambda x: x.VL_PRECO_CUSTO.apply(
                lambda y: float(y.replace(",", "."))),
            VL_PERCENTUAL_LUCRO=lambda x: x.VL_PERCENTUAL_LUCRO.apply(
                lambda y: float(y.replace(",", "."))),
            DT_CADASTRO=lambda x: x.DT_CADASTRO.apply(
                lambda y: y[:10]),
            DT_INICIO=lambda x: pd.to_datetime(x.DT_CADASTRO),
            DT_FIM=lambda x: pd.to_datetime('2023-01-01')).
            assign(
            DS_CATEGORIA=lambda x: x.NO_PRODUTO.apply(
                lambda y: classificar_produto(y))).
            assign(
            DT_CADASTRO=lambda x: pd.to_datetime(x.DT_CADASTRO),
            NO_PRODUTO=lambda x: x.NO_PRODUTO.astype(str),
            FL_ATIVO=lambda x: x.FL_ATIVO.astype("int64"))
    )

    dim_produto.insert(0, 'SK_PRODUTO', range(1, 1 + len(dim_produto)))

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
            VL_PRECO_CUSTO=lambda x:
            x.VL_PRECO_CUSTO.apply(lambda y:
                                   float(y.replace(",", "."))),
            VL_PERCENTUAL_LUCRO=lambda x:
            x.VL_PERCENTUAL_LUCRO.apply(lambda y:
                                        float(y.replace(",", "."))),
            DT_CADASTRO=lambda x: x.DT_CADASTRO.apply(
                lambda y: y[:10])).
            assign(DT_CADASTRO=lambda x: x.DT_CADASTRO.astype("datetime64"))
    )

    df_dw = extract_dim_produto(conn)

    new_records = (
        sqldf("SELECT df_stage.*\
                    FROM df_stage\
                    LEFT JOIN df_dw \
                    ON (df_stage.CD_PRODUTO = df_dw.CD_PRODUTO)\
                    WHERE df_dw.CD_PRODUTO IS NULL").
        assign(
            fl_tipo_update=1
        )
    )

    new_updates = (
        sqldf(f"select stg.* \
                    from df_stage stg \
                    inner join df_dw dw \
                    on stg.CD_PRODUTO = dw.CD_PRODUTO \
                    where stg.VL_PRECO_CUSTO != dw.VL_PRECO_CUSTO\
                    or stg.VL_PERCENTUAL_LUCRO != dw.VL_PERCENTUAL_LUCRO").
            assign(
            fl_tipo_update=2
        )
    )

    new_names = (
        sqldf(f"select stg.* \
                        from df_stage stg \
                        inner join df_dw dw \
                        on stg.CD_PRODUTO = dw.CD_PRODUTO \
                        where stg.NO_PRODUTO != dw.NO_PRODUTO").
        assign(
            fl_tipo_update=3
        )
    )

    new_values = (
        pd.concat([new_records, new_updates, new_names]).assign(
            df_size=df_dw['SK_PRODUTO'].max() + 1
        )
    )

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
        "CD_PRODUTO",
        "NO_PRODUTO",
        "CD_BARRA",
        "DT_CADASTRO",
        "VL_PRECO_CUSTO",
        "VL_PERCENTUAL_LUCRO"
    ]

    size = new_values['df_size'].max()
    new_names = new_values.query('fl_tipo_update == 3')

    if len(new_values) > 0:
        for cd in new_names['CD_PRODUTO']:
            nome = new_names.query(f"CD_PRODUTO == {cd}")["NO_PRODUTO"].item()
            sql = f'update "DW"."D_PRODUTO"\
                set "NO_PRODUTO" = \'{str(nome)}\'\
                 where "CD_PRODUTO" = {cd} and "FL_ATIVO" = 1;'
            conn.execute(sql)

    # extraindo linhas que serão atualizadas
    trated_values = (
        new_values.
        query('fl_tipo_update != 3').
        filter(select_columns).
        assign(
            DT_INICIO=lambda x: pd.to_datetime("today"),
            DT_FIM=lambda x: pd.to_datetime('2023-01-01'),
            FL_ATIVO=lambda x: 1,
            DT_CADASTRO=lambda x: x.DT_CADASTRO.astype("datetime64"),
            DS_CATEGORIA=lambda x: x.NO_PRODUTO.apply(
                lambda y: classificar_produto(y))
        )
    )

    trated_values.insert(0, 'SK_PRODUTO', range(size, size + len(new_values)))

    # identificando as sks que precisam ser atualizadas
    for cd in trated_values['CD_PRODUTO']:
        sql = f'update "DW"."D_PRODUTO"\
                set "FL_ATIVO" = {0},\
                "DT_FIM" = \'{pd.to_datetime("today")}\'\
                where "CD_PRODUTO" = {cd} and "FL_ATIVO" = 1;'
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
        "SK_PRODUTO": Integer(),
        "CD_PRODUTO": Integer(),
        "NO_PRODUTO": String(),
        "CD_BARRA": String(),
        "VL_PRECO_CUSTO": Float(),
        "VL_PERCENTUAL_LUCRO": Float(),
        "DT_CADASTRO": DateTime(),
        "FL_ATIVO": Integer(),
        "DT_INICIO": DateTime(),
        "DT_FIM": DateTime(),
        "DS_CATEGORIA": String()
    }

    (
        dim_produto.
            astype('string').
            to_sql(
            con=conn,
            name='D_PRODUTO',
            schema='DW',
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

