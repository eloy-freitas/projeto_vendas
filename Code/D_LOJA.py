import pandas as pd
import datetime as dt
import time as t
from sqlalchemy.types import DateTime, String, Integer
from pandasql import sqldf
import DW_TOOLS as dwt
from CONEXAO import create_connection_postgre


def extract_stage_loja(conn):
    """
    Extrai todas as tabelas necessárias para gerar a dimensão loja

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_loja_endereco -- pandas.Dataframe;
    """
    stg_loja = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STG_LOJA',
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
        schema='STAGE',
        table_name='STG_ENDERECO',
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

    return stg_loja_endereco


def extract_dim_loja(conn):
    """
    Extrai os dados da dimensão loja

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_loja -- pandas.Dataframe;
    """
    try:
        dim_loja = dwt.read_table(
            conn=conn,
            schema='DW',
            table_name='D_LOJA',
            columns=[
                'SK_LOJA',
                'CD_LOJA',
                'NO_LOJA',
                'DS_RAZAO_SOCIAL',
                'NU_CNPJ',
                'NU_TELEFONE',
                'CD_ENDERECO_LOJA',
                'NO_ESTADO',
                'NO_CIDADE',
                'NO_BAIRRO',
                'DS_RUA',
                'FL_ATIVO',
                'DT_INICIO',
                'DT_FIM'

            ],
            where='"SK_LOJA" > 0'
        )

        return dim_loja
    except:
        return None


def treat_dim_loja(stg_loja_endereco):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_loja_endereco -- pandas.Dataframe;

    return:
    dim_loja -- pandas.Dataframe;
    """
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
        "id_endereco",
        "estado",
        "cidade",
        "bairro",
        "rua"
    ]
    dim_loja = (
        stg_loja_endereco.
            filter(select_columns).
            rename(columns=columns_names).
            assign(
                CD_LOJA=lambda x: x.CD_LOJA.astype("int64"),
                FL_ATIVO=lambda x: 1,
                DT_INICIO=lambda x: dt.date(1900, 1, 1),
                DT_FIM=lambda x: pd.to_datetime('2023-01-01'))
    )

    dim_loja.insert(0, 'SK_LOJA', range(1, 1 + len(dim_loja)))
    print(dim_loja.columns)
    dim_loja = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", "Não informado", "Não informado", -1,"Não informado", "Não informado", "Não informado", "Não informado", -1, None, None],
            [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável", -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável", -2, None, None],
            [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido", -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido", -3, None, None]
        ], columns=dim_loja.columns).append(dim_loja)
    )

    return dim_loja


def extract_new_records(conn):
    """
    Extrai novos registros e registros atualizados na stage

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    new_values: dataframe com as atualizações
    """
    # extraindo os dados da stage
    df_stage = extract_stage_loja(conn)

    # extraindo os dados do dw
    df_dw = extract_dim_loja(conn)

    # fazendo a diferença da stage com o dw, para saber os dados que atualizaram
    new_records = (
        sqldf("SELECT df_stage.*\
                    FROM df_stage\
                    LEFT JOIN df_dw \
                    ON df_stage.id_loja = df_dw.CD_LOJA\
                    WHERE df_dw.CD_LOJA IS NULL").
        assign(
            fl_tipo_update=1
        )
    )

    new_updates = (
        sqldf(f"SELECT stg.* \
                    FROM df_stage stg \
                    INNER JOIN df_dw dw \
                    ON stg.id_loja = dw.CD_LOJA \
                    WHERE \
                    stg.estado != dw.NO_ESTADO\
                    OR stg.cidade != dw.NO_CIDADE\
                    OR stg.bairro != dw.NO_BAIRRO \
                    OR stg.rua != dw.DS_RUA").
        assign(
            fl_tipo_update=2
        )
    )

    new_names = (
        sqldf(f"SELECT stg.* \
                        FROM df_stage stg \
                        INNER JOIN df_dw dw \
                        ON stg.id_loja = dw.CD_LOJA \
                        WHERE stg.nome_loja != dw.NO_LOJA").
        assign(
            fl_tipo_update=3
        )
    )
    new_values = (
        pd.concat([new_records, new_updates, new_names]).assign(
            df_size=df_dw['SK_LOJA'].max() + 1
        )
    )
    return new_values


def treat_updated_loja(new_values, conn):
    """
    Faz o tratamento dos fluxos de execução da SCD loja

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    insert_records ou updated_values -- pandas.Dataframe;
    """
    select_columns = [
        "CD_LOJA",
        "NO_LOJA",
        "DS_RAZAO_SOCIAL",
        "NU_CNPJ",
        "NU_TELEFONE",
        "CD_ENDERECO_LOJA",
        "NO_ESTADO",
        "NO_CIDADE",
        "NO_BAIRRO",
        "DS_RUA"
    ]

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
    size = new_values['df_size'].max()

    new_names = new_values.query('fl_tipo_update == 3')
    if len(new_names) > 0:
        for cd in new_names['id_loja']:
            nome = new_values.query(f"id_loja == {cd}")["id_loja"].item()
            sql = f'UPDATE "DW"."D_LOJA"\
                           SET "NO_LOJA" = \'{str(nome)}\'\
                           WHERE "CD_LOJA" = {cd} AND "FL_ATIVO" = 1;'
            conn.execute(sql)


    # extraindo as linhas que foram alteradas e padronizando os dados
    trated_values = (
        new_values.
            query('fl_tipo_update != 3').
            rename(columns=columns_names).
            filter(items=select_columns).
            assign(
                DT_INICIO=lambda x: pd.to_datetime("today"),
                DT_FIM=lambda x: pd.to_datetime('2023-01-01'),
                FL_ATIVO=lambda x: 1)
    )
    trated_values.insert(0, 'SK_LOJA', range(size, size + len(new_values)))

    # atualizando a flag e data_fim dos dados atualizados
    for cd in trated_values['CD_LOJA']:
        sql = f'UPDATE "DW"."D_LOJA"\
            SET "FL_ATIVO" = {0},\
            "DT_FIM" = \'{pd.to_datetime("today")}\'\
            WHERE "CD_LOJA" = {cd} AND "FL_ATIVO" = 1;'
        conn.execute(sql)

    return trated_values


def load_dim_loja(dim_loja, conn, action):
    """
    Faz a carga da dimensão loja no DW.

    parâmetros:
    dim_loja -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    action -- if_exists (append, replace...)
    """
    data_types = {
        "SK_LOJA": Integer(),
        "CD_LOJA": Integer(),
        "NO_LOJA": String(),
        "DS_RAZAO_SOCIAL": String(),
        "NU_CNPJ": String(),
        "NU_TELEFONE": String(),
        "CD_ENDERECO_LOJA":Integer(),
        "NO_ESTADO": String(),
        "NO_CIDADE": String(),
        "NO_BAIRRO": String,
        "DS_RUA": String(),
        "FL_ATIVO": Integer(),
        "DT_INICIO": DateTime(),
        "DT_FIM": DateTime()
    }
    (
        dim_loja.
            astype('string').
            to_sql(
            con=conn,
            name='D_LOJA',
            schema='DW',
            if_exists=action,
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )


def run_dim_loja(conn):
    """
    Executa o pipeline da dimensão loja.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    dim_loja = extract_dim_loja(conn)
    if dim_loja is None:
        (

            extract_stage_loja(conn=conn).
                pipe(treat_dim_loja).
                pipe(load_dim_loja, conn=conn, action='replace')
        )
    else:
        (
            extract_new_records(conn).
                pipe(treat_updated_loja, conn=conn).
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
