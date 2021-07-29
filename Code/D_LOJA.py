import pandas as pd
import datetime as dt
from sqlalchemy.types import DateTime, String, Integer
from pandasql import sqldf
import DW_TOOLS as dwt


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
        table_name='STG_LOJA'
    )

    stg_endereco = dwt.read_table(
        conn=conn,
        schema='STAGE',
        table_name='STG_ENDERECO'
    )

    stg_loja_endereco = (
        stg_loja.pipe(
            pd.merge,
            right=stg_endereco,
            left_on="id_endereco",
            right_on="id_endereco",
            suffixes=["_01", "_02"],
            how='inner'
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

    dim_loja = (
        pd.DataFrame([
            [-1, -1, "Não informado", "Não informado", -1, -1, "Não informado", "Não informado", "Não informado", "Não informado", -1, None, None],
            [-2, -2, "Não aplicável", "Não aplicável", -2, -2, "Não aplicável", "Não aplicável", "Não aplicável", "Não aplicável", -2, None, None],
            [-3, -3, "Desconhecido", "Desconhecido", -3, -3, "Desconhecido", "Desconhecido", "Desconhecido", "Desconhecido", -3, None, None]
        ], columns=dim_loja.columns).append(dim_loja)
    )

    return dim_loja


def treat_updated_loja(conn):
    """
    Faz o tratamento dos fluxos de execução da SCD loja

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    insert_records ou updated_values -- pandas.Dataframe;
    """
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

    # extraindo os dados da stage
    stg_source = extract_stage_loja(conn).rename(columns=columns_names)

    # extraindo os dados do dw
    dw_source = extract_dim_loja(conn).rename(columns=columns_names)

    df_dw = dw_source.filter(items=select_columns)
    df_stage = stg_source.filter(items=select_columns)
    # fazendo a diferença da stage com o dw, para saber os dados que atualizaram

    new_records = (
        sqldf("SELECT df_stage.*\
                    FROM df_stage\
                    LEFT JOIN df_dw \
                    ON (df_stage.CD_LOJA = df_dw.CD_LOJA)\
                    WHERE df_dw.CD_LOJA IS NULL")
    )

    new_updates = (
        sqldf(f"select stg.* \
                    from df_stage stg \
                    inner join df_dw dw \
                    on stg.CD_LOJA = dw.CD_LOJA \
                    where stg.NO_ESTADO != dw.NO_ESTADO\
                    or stg.NO_CIDADE != dw.NO_CIDADE\
                    or stg.NO_BAIRRO != dw.NO_BAIRRO \
                    or stg.DS_RUA != dw.DS_RUA")
    )

    names_updateds = (
        sqldf(f"select stg.* \
                        from df_stage stg \
                        inner join df_dw dw \
                        on stg.CD_LOJA = dw.CD_LOJA \
                        where stg.NO_LOJA != dw.NO_LOJA")
    )

    size = dw_source['SK_LOJA'].max() + 1
    if len(new_updates) > 0 or len(names_updateds) > 0:
        # extraindo as linhas que foram alteradas e padronizando os dados
        updated_values = (
            new_updates.
                assign(
                DT_INICIO=lambda x: pd.to_datetime("today"),
                DT_FIM=lambda x: pd.to_datetime('2023-01-01'),
                FL_ATIVO=lambda x: 1
            )
        )
        updated_values.insert(0, 'SK_LOJA', range(size, size + len(new_updates)))

        # atualizando a flag e data_fim dos dados atualizados
        for sk in updated_values['CD_LOJA']:
            sql = f'update "DW"."D_LOJA"\
                set "FL_ATIVO" = {0},\
                "DT_FIM" = \'{pd.to_datetime("today")}\'\
                where "CD_LOJA" = {sk} and "FL_ATIVO" = 1;'
            conn.execute(sql)

        for cd in names_updateds['CD_LOJA']:
            nome = names_updateds.query(f"CD_LOJA == {cd}")["NO_LOJA"].item()
            sql = f'update "DW"."D_LOJA"\
                       set "NO_LOJA" = \'{str(nome)}\'\
                        where "CD_LOJA" = {cd} and "FL_ATIVO" = 1;'
            conn.execute(sql)

        return updated_values
    if len(new_records) > 0:
        insert_records = (
            new_records.assign(
            DT_INICIO=lambda x: pd.to_datetime('today'),
            DT_FIM=lambda x: pd.to_datetime('2023-01-01'),
            FL_ATIVO=lambda x: 1
            )
        )

        insert_records.insert(0, 'SK_LOJA', range(size, size + len(new_records)))
        return insert_records


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
            treat_updated_loja(conn).
                pipe(load_dim_loja, conn=conn, action='append')

        )


