import pandas as pd
import datetime as dt
import time as t
from sqlalchemy import Integer
from sqlalchemy.types import String
from sqlalchemy.types import Date
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt

columns_names = {
    "id_loja": "CD_LOJA",
    "nome_loja": "NO_LOJA",
    "razao_social": "DS_RAZAO_SOCIAL",
    "cnpj": "NU_CNPJ",
    "telefone": "NU_TELEFONE",
    "id_endereco": "CD_ENDERECO_LOJA",
    "estado": "NO_ESTADO",
    "cidade": "NO_CIDADE",
    "bairro": "NO_BAIRRO",
    "rua": "DS_RUA"
}

select_columns = [
    "id_loja",
    "nome_loja",
    "razao_social",
    "cnpj",
    "telefone",
    "estado",
    "cidade",
    "bairro",
    "rua"
]


def extract_stage_loja(conn):
    stage_loja = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_LOJA'
    )

    stage_endereco = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STAGE_ENDERECO'
    )

    stage_loja = (
        stage_loja.pipe(
            pd.merge,
            right=stage_endereco,
            left_on="id_endereco",
            right_on="id_endereco",
            suffixes=["_01", "_02"],
            how='inner'
        )
    )
    
    return stage_loja


def extract_dim_loja(conn):
    dim_loja = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='D_LOJA',
        where='"SK_LOJA" > 0'
    )

    return dim_loja


def treat_dim_loja(stage_loja):
    dim_loja = (
        stage_loja.
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
            [-1, -1, "Não informado", "Não informado", -1, -1, "Não informado", "Não informado", "Não informado", "Não informado", -1, None, None],
            [-2, -2, "Não aplicável", "Não aplicável", -2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável", -2, None, None],
            [-3, -3, "Desconhecido", "Desconhecido", -3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido", -3, None, None]
        ], columns=dim_loja.columns).append(dim_loja)
    )
    print(dim_loja.columns)
    return dim_loja


def get_new_loja(conn):
    df_stage = extract_stage_loja(conn)

    df_dw = extract_dim_loja(conn)

    join_df = (
        pd.merge(
            left=df_stage,
            right=df_dw,
            left_on='id_loja',
            right_on="CD_LOJA",
            how='left').
            assign(
            FL_INSERT=lambda x: x.CD_LOJA.apply(
                lambda y: 'I' if pd.isnull(y) else 'N')
        )
    )

    max_cd_dw = df_dw['SK_LOJA'].max() + 1
    size = max_cd_dw + join_df.query(f'FL_INSERT == "I"').shape[0] - max_cd_dw
    columns = [
        "SK_LOJA",
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
        "FL_ATIVO",
        "DT_INICIO",
        "DT_FIM"]

    insert_record = (
        join_df.
            query("FL_INSERT == 'I'")[columns].
            filter(items=select_columns).
            rename(columns=columns_names).
            assign(
                SK_LOJA=lambda x: range(max_cd_dw,
                                       (max_cd_dw + size)),
                FL_ATIVO=lambda x: 1,
                DT_INICIO=lambda x: pd.to_datetime('today'),
                DT_FIM=lambda x: None)

    )

    return insert_record


def treat_updated_loja(conn):
    select_columns = {
        "CD_LOJA",
        "NO_LOJA",
        "DS_RAZAO_SOCIAL",
        "NU_CNPJ",
        "NU_TELEFONE",
        "NO_ESTADO",
        "NO_CIDADE",
        "NO_BAIRRO",
        "DS_RUA"
    }

    # extraindo os dados da stage
    df_stage = extract_stage_loja(conn).rename(columns=columns_names)

    # extraindo os dados do dw
    df_dw = extract_dim_loja(conn).rename(columns=columns_names)

    # fazendo a diferença da stage com o dw, para saber os dados que atualizaram

    diference = (
        df_dw.
            filter(items=select_columns).sort_index().
            compare(df_stage.filter(items=select_columns).sort_index(),
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
    updated_values.to_sql(
        con=conn,
        name='D_LOJA',
        schema='DW',
        if_exists='append',
        index=False,
        chunksize=100
    )


def load_dim_loja(dim_loja, conn):
    data_types = {
        "SK_LOJA": Integer(),
        "CD_LOJA": Integer(),
        "NO_LOJA": String(),
        "DS_RAZAO_SOCIAL": String(),
        "NU_CNPJ": String(),
        "NU_TELEFONE": String(),
        "NO_ESTADO": String(),
        "NO_CIDADE": String(),
        "NO_BAIRRO": String,
        "DS_RUA": String(),
        "FL_ATIVO": Integer(),
        "DT_INICIO": Date(),
        "DT_FIM": Date()
    }

    (
        dim_loja.
            astype('string').
            to_sql(
            con=conn,
            name='D_LOJA',
            schema='DW',
            if_exists='replace',
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )



def run_updated_loja(conn):
    (
        treat_updated_loja(conn).
            pipe(load_updated_loja, conn=conn)
    )


def run_new_loja(conn):
    (
        get_new_loja(conn).
        pipe(load_updated_loja, conn=conn)
    )


def run_dim_loja(conn):
    (
        extract_stage_loja(conn=conn).
            pipe(treat_dim_loja)#.
            #pipe(load_dim_loja, conn=conn)
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
    #run_new_loja(conn_dw)
    #run_updated_loja(conn_dw)
    run_dim_loja(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
