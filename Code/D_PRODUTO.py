import pandas as pd
import datetime as dt
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data, get_data_from_database


def extract_dim_produto(conn):
    dim_produto = get_data_from_database(
        conn_input=conn,
        sql_query=f'select * from "STAGES"."STAGE_PRODUTO";'
    )

    return dim_produto


def treat_dim_produto(dim_produto):
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
                           "EXTRATO DE TOMATE", "AÇUCAR 5 KG",
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
            DT_CADASTRO=lambda x: x.DT_CADASTRO.astype("datetime64"),
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

    print(dim_produto)
    return dim_produto


def load_dim_produto(dim_produto, conn):
    insert_data(
        data=dim_produto,
        connection=conn,
        table_name='D_PRODUTO',
        schema_name='DW',
        action='replace'
    )


def run_dim_produto(conn):
    (
        extract_dim_produto(conn).
            pipe(treat_dim_produto).
            pipe(load_dim_produto, conn=conn)
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
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
