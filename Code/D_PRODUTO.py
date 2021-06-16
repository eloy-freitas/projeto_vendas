import numpy as np
import pandas as pd
import datetime as dt
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database, merge_input

columns_names = {
    "id_produto": "CD_PRODUTO",
    "nome_produto": "NO_PRODUTO",
    "cod_barra": "CD_BARRA",
    "preco_custo": "VL_PRECO_CUSTO",
    "percentual_lucro": "VL_PERCENTUAL_LUCRO",
    "data_cadastro": "DT_CADASTRO",
    "ativo": "FL_ATIVO"
}

categoria_cafe_manha = {"CAFE", "ACHOCOLATADO", "CEREAIS", "PÃO",
                        "AÇUCAR", "SUCO", "ADOÇANTE", "BISCOITO",
                        "GELEIA", "IOGURTE"}

categoria_mercearia = {"ARROZ", "FEIJÃO", "FARINHA DE TRIGO",
                       "AMIDO DE MILHO", "FERMENTO", "MACARRÃO",
                       "MOLHO DE TOMATE", "AZEITE", "ÓLEO DE SOJA",
                       "OVOS", "TEMPERO", "SAL", "FARINHA DE AVEIA",
                       "EXTRATO DE TOMATE", "AÇUCAR 4 KG",
                       "SAZON SABOR CARNE", "SAZON SABOR FRANGO"}

categoria_carnes = {"BIFE DE BOI", "FRANGO", "PEIXE", "CARNE MOIDA",
                    "SALSICHA", "LINGUIÇA"}

categoria_bebidas = {"SUCO", "CERVEJA", "REFRIGERANTE", "VINHO"}

categoria_higiene = {"SABONETE", "CREME DENTAL", "SHAMPOO",
                     "CONDICIONADOR", "ABSORVENTE", "PAPEL HIGIÊNICO",
                     "FRALDA"}

categoria_frios = {"LEITE", "PRESUNTO", "QUEIJO", "REQUEIJÃO",
                   "MANTEIGA", "CREME DE LEITE"}

categoria_limpeza = {"AGUA SANITARIA", "SABÃO EM PÓ", "PALHA DE AÇO",
                     "AMACIANTE", "DETERGENTE", "SACO DE LIXO",
                     "DESINFETANTE", "PAPEL TOALHA", "PAPEL HIGIENICO"}

categoria_hortifruti = {"ALFACE", "CEBOLA", "ALHO", "TOMATE",
                        "LIMÃO", "BANANA", "MAÇÃ", "BATATA",
                        "BATATA DOCE", "BATATA INGLESA"}


def extract_dim_produto(conn):
    dim_produto = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_PRODUTO";'
    )

    return dim_produto


def treat_dim_produto(dim_produto):
    dim_produto = (
        dim_produto.
            rename(columns=columns_names).
            assign(
            VL_PRECO_CUSTO=lambda x: x.VL_PRECO_CUSTO.apply(
                lambda y: float(y.replace(",", "."))),
            VL_PERCENTUAL_LUCRO=lambda x: x.VL_PERCENTUAL_LUCRO.apply(
                lambda y: float(y.replace(",", "."))),
            DT_CADASTRO=lambda x: x.DT_CADASTRO.apply(
                lambda y: y[:10]),
            DT_INICIO=lambda x: dt.date(1900, 1, 1),
            DT_FIM=lambda x: None).
            assign(
            CD_CATEGORIA=lambda x: x.NO_PRODUTO.map(
                lambda y:
                1 if y in categoria_cafe_manha else
                2 if y in categoria_mercearia else
                3 if y in categoria_carnes else
                4 if y in categoria_bebidas else
                5 if y in categoria_higiene else
                6 if y in categoria_frios else
                7 if y in categoria_limpeza else
                8 if y in categoria_hortifruti else -1)).
            assign(
            DT_CADASTRO=lambda x: x.DT_CADASTRO.astype(str),
            DT_INICIO= lambda x: x.DT_INICIO.astype(str),
            DT_FIM=lambda x: x.DT_FIM.astype(str),
            NO_PRODUTO=lambda x: x.NO_PRODUTO.astype(str),
            FL_ATIVO=lambda x: x.FL_ATIVO.astype("int64"),
            CD_CATEGORIA=lambda x: x.CD_CATEGORIA.astype("int64"))
    )

    dim_produto.insert(0, 'SK_PRODUTO', range(1, 1 + len(dim_produto)))

    dim_produto = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, -1, -1, -1, -1, -1, -1],
            [-2, -2, "Não aplicável", -2, -2, -2, -2, -2, -2, -2, -2],
            [-3, -3, "Desconhecido", -3, -3, -3, -3, -3, -3, -3, -3]
        ], columns=dim_produto.columns).append(dim_produto)
    )

    return dim_produto


def load_dim_produto(dim_produto, conn):
    insert_data(
        data=dim_produto,
        connection=conn,
        table_name='D_PRODUTO',
        schema_name='DW',
        action='replace'
    )


def insert_new_produto(conn):
    df_stage = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_PRODUTO";'
    )

    df_dw = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "DW"."D_PRODUTO";'
    )

    join_df = (
        pd.merge(
            left=df_stage,
            right=df_dw,
            left_on='id_produto',
            right_on="CD_PRODUTO",
            how='left').
            assign(
            FL_INSERT=lambda x: x.CD_PRODUTO.apply(
                lambda y: 'I' if pd.isnull(y) else 'N'
            )
        )
    )

    insert_record = (
        join_df.
            query("FL_INSERT == 'I'")[[
            "SK_PRODUTO",
            "id_produto",
            "nome_produto",
            "cod_barra",
            "preco_custo",
            "percentual_lucro",
            "data_cadastro",
            "ativo",
            "DT_INICIO",
            "DT_FIM"]].
            rename(columns=columns_names)

    )

    df_size = df_dw['SK_PRODUTO'].max() + 1
    insert_record = (
        insert_record.assign(
            DT_INICIO=lambda x: dt.date(1900, 1, 1),
            DT_FIM=lambda x: None,
            SK_PRODUTO=lambda x: range(df_size,
                                       df_size + len(insert_record)),
            VL_PRECO_CUSTO=lambda x: x.VL_PRECO_CUSTO.apply(
                lambda y: float(y.replace(",", "."))),
            VL_PERCENTUAL_LUCRO=lambda x: x.VL_PERCENTUAL_LUCRO.apply(
                lambda y: float(y.replace(",", ".")))).
            assign(
            CD_CATEGORIA=lambda x: x.NO_PRODUTO.map(
                lambda y:
                1 if y in categoria_cafe_manha else
                2 if y in categoria_mercearia else
                3 if y in categoria_carnes else
                4 if y in categoria_bebidas else
                5 if y in categoria_higiene else
                6 if y in categoria_frios else
                7 if y in categoria_limpeza else
                8 if y in categoria_hortifruti else -1)).
            assign(
            DT_CADASTRO=lambda x: x.DT_CADASTRO.astype("datetime64"),
            NO_PRODUTO=lambda x: x.NO_PRODUTO.astype(str),
            FL_ATIVO=lambda x: x.FL_ATIVO.astype("int64"),
            CD_CATEGORIA=lambda x: x.CD_CATEGORIA.astype("int64"))
    )

    return insert_record


def get_updated_produto(conn):
    select_columns = [
        "CD_PRODUTO",
        "NO_PRODUTO",
        "CD_BARRA",
        "VL_PRECO_CUSTO",
        "VL_PERCENTUAL_LUCRO"
    ]

    df_stage = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_PRODUTO"\
        order by "id_produto";').rename(
        columns=columns_names).assign(
        VL_PRECO_CUSTO=lambda x:
        x.VL_PRECO_CUSTO.apply(lambda y:
                               float(y.replace(",", "."))),
        VL_PERCENTUAL_LUCRO=lambda x:
        x.VL_PERCENTUAL_LUCRO.apply(lambda y:
                                    float(y.replace(",", "."))))

    df_dw = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "DW"."D_PRODUTO" where \
        "CD_PRODUTO" > 0 order by "SK_PRODUTO";'
    )

    # descobrindo quais linhas foram alteradas
    diference = (
        df_dw.filter(items=select_columns).
            compare(
            df_stage.filter(items=select_columns),
            align_axis=0,
            keep_shape=False
        )
    )

    # identificando index das linhas alteradas
    indexes = {x[0] for x in diference.index}
    size = df_dw['SK_PRODUTO'].max() + 1

    # extraindo linhas que serão atualizadas
    updated_values = (
        df_dw.loc[indexes].
            assign(
            VL_PRECO_CUSTO=lambda x: diference.
                iloc[1]['VL_PRECO_CUSTO'],
            VL_PERCENTUAL_LUCRO=diference.
                iloc[1]['VL_PERCENTUAL_LUCRO']).
            assign(
            SK_PRODUTO=lambda x: range(size, size
                                       + len(indexes)),
            CD_PRODUTO=lambda x: df_dw.loc[indexes]['CD_PRODUTO'],
            DT_INICIO=lambda x: pd.to_datetime("today"),
            DT_FIM=lambda x: None,
            FL_ATIVO=lambda x: 1
        )
    )

    #identificando as sks que precisam ser atualizadas
    set_to_update = list(df_dw['SK_PRODUTO'].loc[indexes])

    for sk in set_to_update:
        sql = f'update "DW"."D_PRODUTO"\
            set "FL_ATIVO" = {0},\
            "DT_FIM" = \'{pd.to_datetime("today")}\'\
            where "SK_PRODUTO" = {sk};'
        conn.execute(sql)

    return updated_values


def load_new_produto(insert_record, conn):
    insert_data(
        data=insert_record,
        connection=conn,
        schema_name="DW",
        table_name="D_PRODUTO",
        action='append'
    )


def run_dim_produto(conn):
    (
        extract_dim_produto(conn).
            pipe(treat_dim_produto).
            pipe(load_dim_produto, conn=conn)
    )


def run_new_produto(conn):
    (
        insert_new_produto(conn).
            pipe(load_new_produto, conn=conn)
    )


def run_update_produto(conn):
    (
        get_updated_produto(conn).
        pipe(load_new_produto, conn=conn)
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
    #run_dim_produto(conn_dw)
    #run_new_produto(conn_dw)
    run_update_produto(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
