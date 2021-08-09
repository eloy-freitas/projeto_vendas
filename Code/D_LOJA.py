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
            schema='dw',
            table_name='d_loja',
            columns=[
                'sk_loja',
                'cd_loja',
                'no_loja',
                'ds_razao_social',
                'nu_cnpj',
                'nu_telefone',
                'cd_endereco_loja',
                'no_estado',
                'no_cidade',
                'no_bairro',
                'ds_rua',
                'fl_ativo',
                'dt_inicio',
                'dt_fim'],
            where='"sk_loja" > 0'
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
        "rua"
    ]
    dim_loja = (
        stg_loja_endereco.
        filter(select_columns).
        rename(columns=columns_names).
        assign(
            cd_loja=lambda x: x.cd_loja.astype("int64"),
            fl_ativo=lambda x: 1,
            dt_inicio=lambda x: dt.date(1900, 1, 1),
            dt_fim=None
        )
    )

    dim_loja.insert(0, 'sk_loja', range(1, 1 + len(dim_loja)))

    dim_loja = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", "Não informado", "Não informado", -1, "Não informado",
             "Não informado", "Não informado", "Não informado", -1, None, None],
            [-2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável", -2, "Não aplicável",
             "Não aplicável", "Não aplicável", "Não aplicável", -2, None, None],
            [-3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido", -3, "Desconhecido", "Desconhecido",
             "Desconhecido", "Desconhecido", -3, None, None]
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
        sqldf("\
            SELECT df_stage.*\
            FROM df_stage\
            LEFT JOIN df_dw \
            ON df_stage.id_loja = df_dw.cd_loja\
            WHERE df_dw.cd_loja IS NULL").
        assign(
            fl_tipo_update=1
        )
    )

    new_updates = (
        sqldf("\
            SELECT stg.*\
            FROM df_stage stg\
            INNER JOIN df_dw dw\
            ON stg.id_loja = dw.cd_loja\
            WHERE\
            stg.estado != dw.no_estado\
            OR stg.cidade != dw.no_cidade\
            OR stg.bairro != dw.no_bairro\
            OR stg.rua != dw.ds_rua").
        assign(
            fl_tipo_update=2
        )
    )

    new_names = (
        sqldf("\
            SELECT stg.* \
            FROM df_stage stg \
            INNER JOIN df_dw dw \
            ON stg.id_loja = dw.cd_loja \
            WHERE stg.nome_loja != dw.no_loja").
        assign(
            fl_tipo_update=3
        )
    )
    new_values = (
        pd.concat([new_records, new_updates, new_names]).assign(
            df_size=df_dw['sk_loja'].max() + 1
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
        'cd_loja',
        'no_loja',
        'ds_razao_social',
        'nu_cnpj',
        'nu_telefone',
        'cd_endereco_loja',
        'no_estado',
        'no_cidade',
        'no_bairro',
        'ds_rua',
    ]

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

    size = new_values['df_size'].max()

    new_names = new_values.query('fl_tipo_update == 3')
    if len(new_names) > 0:
        for cd in new_names['id_loja']:
            nome = new_values.query(f"id_loja == {cd}")["id_loja"].item()
            sql = (f'\
                UPDATE "dw"."d_loja"\
                SET "no_loja" = \'{str(nome)}\'\
                WHERE "cd_loja" = {cd} AND "fl_ativo" = 1;')
            conn.execute(sql)

    # extraindo as linhas que foram alteradas e padronizando os dados
    trated_values = (
        new_values.
        query('fl_tipo_update != 3').
        rename(columns=columns_names).
        filter(items=select_columns).
        assign(
            dt_inicio=lambda x: pd.to_datetime("today"),
            dt_fim=None,
            fl_ativo=lambda x: 1)
    )
    trated_values.insert(0, 'sk_loja', range(size, size + len(new_values)))

    # atualizando a flag e data_fim dos dados atualizados

    for cd in trated_values['cd_loja']:
        sql = (f'\
            UPDATE "dw"."d_loja"\
            SET "fl_ativo" = {0},\
            "dt_fim" = \'{pd.to_datetime("today")}\'\
            WHERE "cd_loja" = {cd} AND "fl_ativo" = 1;')
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
