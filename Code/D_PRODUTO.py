
import pandas as pd
import datetime as dt
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data
import DW_TOOLS as dwt

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
                   "MANTEIGA", "CREME DE LEITE", "MANTEIGA SEM SAL"}

categoria_limpeza = {"AGUA SANITARIA", "SABÃO EM PÓ", "PALHA DE AÇO",
                     "AMACIANTE", "DETERGENTE", "SACO DE LIXO",
                     "DESINFETANTE", "PAPEL TOALHA", "PAPEL HIGIENICO"}

categoria_hortifruti = {"ALFACE", "CEBOLA", "ALHO", "TOMATE",
                        "LIMÃO", "BANANA", "MAÇÃ", "BATATA",
                        "BATATA DOCE", "BATATA INGLESA"}


def extract_dim_produto(conn):
    dim_produto = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_PRODUTO'
    )

    return dim_produto


def treat_dim_produto(dim_produto):
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
        dim_produto.
            filter(select_columns).
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
            DS_CATEGORIA=lambda x: x.NO_PRODUTO.map(
                lambda y:
                "Café da manhã" if y in categoria_cafe_manha else
                "Mercearia" if y in categoria_mercearia else
                "Carnes" if y in categoria_carnes else
                "Bebidas" if y in categoria_bebidas else
                "Higiene" if y in categoria_higiene else
                "Frios" if y in categoria_frios else
                "Limpeza" if y in categoria_limpeza else
                "Hortifruti" if y in categoria_hortifruti else
                "Desconhecido")).
            assign(
            DT_CADASTRO=lambda x: x.DT_CADASTRO.astype(str),
            DT_INICIO=lambda x: x.DT_INICIO.astype(str),
            DT_FIM=lambda x: x.DT_FIM.astype(str),
            NO_PRODUTO=lambda x: x.NO_PRODUTO.astype(str),
            FL_ATIVO=lambda x: x.FL_ATIVO.astype("int64"))
    )

    dim_produto.insert(0, 'SK_PRODUTO', range(1, 1 + len(dim_produto)))

    dim_produto = (
        pd.DataFrame([
            [-1, -1, "Não informado", -1, -1, -1, -1, -1, -1, -1, "Não informado"],
            [-2, -2, "Não aplicável", -2, -2, -2, -2, -2, -2, -2, "Não aplicável"],
            [-3, -3, "Desconhecido", -3, -3, -3, -3, -3, -3, -3, "Desconhecido"]
        ], columns=dim_produto.columns).append(dim_produto)
    )

    return dim_produto


def load_dim_produto(dim_produto, conn):
    dim_produto.to_sql(
        con=conn,
        name='D_PRODUTO',
        schema='DW',
        if_exists='replace',
        index=False,
        chunksize=100
    )


def get_new_produto(conn):
    df_stage = (
            dwt.read_table(
                conn=conn,
                schema='STAGE',
                table_name='STAGE_PRODUTO').
            assign(
            preco_custo=lambda x: x.preco_custo.apply(
                lambda y: float(y.replace(",", "."))),
            percentual_lucro=lambda x: x.percentual_lucro.apply(
                lambda y: float(y.replace(",", ".")))
        )
    )

    df_dw = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_PRODUTO',
        where='"SK_PRODUTO" > 0'
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
                lambda y: 'I' if pd.isnull(y) else 'N')
        )
    )

    max_cd_stage = df_stage['id_produto'].max() + 1
    max_cd_dw = df_dw['CD_PRODUTO'].max() + 1

    columns = [
        "SK_PRODUTO",
        "id_produto",
        "nome_produto",
        "cod_barra",
        "preco_custo",
        "percentual_lucro",
        "data_cadastro",
        "ativo",
        "DT_INICIO",
        "DT_FIM"]

    insert_record = (
        join_df.
            query("FL_INSERT == 'I'")[columns].
            rename(columns=columns_names)
            .assign(
            DT_INICIO=lambda x: dt.date(1900, 1, 1),
            DT_FIM=lambda x: None,
            SK_PRODUTO=lambda x: range(max_cd_dw,
                                       max_cd_dw
                                       + (max_cd_stage
                                          - max_cd_dw))).
            assign(
            DS_CATEGORIA=lambda x: x.NO_PRODUTO.apply(
                lambda y:
                "Café da manhã" if y in categoria_cafe_manha else
                "Mercearia" if y in categoria_mercearia else
                "Carnes" if y in categoria_carnes else
                "Bebidas" if y in categoria_bebidas else
                "Higiene" if y in categoria_higiene else
                "Frios" if y in categoria_frios else
                "Limpeza" if y in categoria_limpeza else
                "Hortifruti" if y in categoria_hortifruti else
                "Desconhecido")).
            assign(
            DT_CADASTRO=lambda x: x.DT_CADASTRO.astype("datetime64"),
            NO_PRODUTO=lambda x: x.NO_PRODUTO.astype(str))

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
    df_stage = (
        dwt.read_table(
            conn=conn,
            schema='STAGE',
            table_name='STAGE_PRODUTO',
            where='id_produto > 0 order by id_produto').
            rename(columns=columns_names).
            assign(
            VL_PRECO_CUSTO=lambda x:
            x.VL_PRECO_CUSTO.apply(lambda y:
                                   float(y.replace(",", "."))),
            VL_PERCENTUAL_LUCRO=lambda x:
            x.VL_PERCENTUAL_LUCRO.apply(lambda y:
                                        float(y.replace(",", "."))))
    )

    df_dw = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_PRODUTO',
        where=f'"CD_PRODUTO" > 0 order by "SK_PRODUTO";'
    )

    # descobrindo quais linhas foram alteradas
    diference = (
        df_dw.
            filter(items=select_columns).
            compare(df_stage.filter(items=select_columns),
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
            SK_PRODUTO=lambda x: range(size, size
                                       + len(indexes)),
            CD_PRODUTO=lambda x: df_dw.loc[indexes]['CD_PRODUTO'],
            DT_INICIO=lambda x: pd.to_datetime("today"),
            DT_FIM=lambda x: None,
            FL_ATIVO=lambda x: 1
        )
    )

    for c in diference.columns:
        updated_values[c] = diference.iloc[1][c]

    # identificando as sks que precisam ser atualizadas
    set_to_update = list(df_dw['CD_PRODUTO'].loc[indexes])

    for sk in set_to_update:
        sql = f'update "DW"."D_PRODUTO"\
            set "FL_ATIVO" = {0},\
            "DT_FIM" = \'{pd.to_datetime("today")}\'\
            where "CD_PRODUTO" = {sk};'
        conn.execute(sql)

    return updated_values


def load_new_produto(insert_record, conn):
    insert_record.to_sql(
        con=conn,
        name='D_PRODUTO',
        schema='DW',
        if_exists='append',
        index=False,
        chunksize=100
    )   


def run_dim_produto(conn):
    (
        extract_dim_produto(conn).
            pipe(treat_dim_produto).
            pipe(load_dim_produto, conn=conn)
    )


def run_new_produto(conn):
    (
        get_new_produto(conn).
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
    run_dim_produto(conn_dw)
    #run_new_produto(conn_dw)
    #run_update_produto(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")