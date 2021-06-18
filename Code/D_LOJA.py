import pandas as pd
import datetime as dt
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data
import DW_TOOLS as dwt

columns_names = {
    "id_loja": "CD_LOJA",
    "nome_loja": "NO_LOJA",
    "razao_social": "DS_RAZAO_SOCIAL",
    "cnpj": "NU_CNPJ",
    "telefone": "NU_TELEFONE",
    "id_endereco": "CD_ENDERECO_LOJA"
}


def extract_dim_loja(conn):
    dim_loja = dwt.read_table(
        conn=conn,
        schema='STAGES',
        table_name='STAGE_LOJA'
    )
    return dim_loja


def treat_dim_loja(dim_loja):
    select_columns = [
        "id_loja",
        "nome_loja",
        "razao_social",
        "cnpj",
        "telefone"
    ]

    dim_loja = (
        dim_loja.
            filter(select_columns).
            rename(columns=columns_names).
            assign(
                CD_LOJA=lambda x: x.CD_LOJA.astype("int64"),
                FL_ATIVO=lambda x: 1,
                DT_INICIO=lambda x: dt.date(1900, 1, 1),
                DT_FIM=lambda x: str(None))
    )

    dim_loja.insert(0, 'SK_LOJA', range(1, 1 + len(dim_loja)))

    dim_loja = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", -1, -1, -1, -1, -1],
            [-2, -2, "Não aplicável", "Não aplicável", -2, -2, -2, -2, -2],
            [-3, -3, "Desconhecido", "Desconhecido", -3, -3, -3, -3, -3]
        ], columns=dim_loja.columns).append(dim_loja)
    )

    return dim_loja


def get_updated_loja(conn):
    select_columns = {
        "CD_LOJA",
        "NO_LOJA",
        "DS_RAZAO_SOCIAL",
        "NU_CNPJ",
        "NU_TELEFONE",
        "CD_ENDERECO_LOJA"
    }

    # extraindo os dados da stage
    df_stage = dwt.read_table(
        conn=conn,
        schema='STAGES',
        table_name='STAGE_LOJA'
    ).rename(columns=columns_names)

    # extraindo os dados do dw
    df_dw = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_LOJA',
        where='"SK_LOJA" > 0'
    )

    # fazendo a diferença da stage com o dw, para saber os dados que atualizaram
    diference = (
        df_dw.
            filter(items=select_columns).
            compare(df_stage.filter(items=select_columns),
                    align_axis=0,
                    keep_shape=False
                    )
    )

    # identificando os indexes que foram alterados
    indexes = {x[0] for x in diference.index}
    size = df_dw['SK_LOJA'].max() + 1

    # extraindo as linhas que foram alteradas e padronizando os dados
    updated_values = (
        df_dw.loc[indexes].
            assign(
            SK_LOJA=lambda x: range(size, size
                                    + len(indexes)),
            CD_LOJA=lambda x: df_dw.loc[indexes]['CD_LOJA'],
            DT_INICIO=lambda x: pd.to_datetime("today"),
            DT_FIM=lambda x: None,
            FL_ATIVO=lambda x: 1
        )
    )

    # atualizando cada coluna que foi atualizada
    for c in diference.columns:
        updated_values[c] = diference.iloc[1][c]

    # identificando as sk que foram alteradas
    set_to_update = list(df_dw['SK_LOJA'].loc[indexes])

    # atualizando a flag e data_fim dos dados atualizados
    for sk in set_to_update:
        sql = f'update "DW"."D_LOJA"\
            set "FL_ATIVO" = {0},\
            "DT_FIM" = \'{pd.to_datetime("today")}\'\
            where "SK_LOJA" = {sk};'
        conn.execute(sql)

    return updated_values


def load_updated_loja(updated_values, conn):
    insert_data(
        data=updated_values,
        connection=conn,
        table_name='D_LOJA',
        schema_name='DW',
        action='append'
    )


def load_dim_loja(dim_loja, conn):
    insert_data(
        data=dim_loja,
        connection=conn,
        table_name='D_LOJA',
        schema_name='DW',
        action='replace'
    )


def run_updated_loja(conn):
    (
        get_updated_loja(conn).
            pipe(load_updated_loja, conn=conn)
    )


def run_dim_loja(conn):
    (
        extract_dim_loja(conn=conn).
            pipe(treat_dim_loja).
            pipe(load_dim_loja, conn=conn)
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
    #run_updated_loja(conn_dw)
    run_dim_loja(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
