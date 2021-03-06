import time as t
import pandas as pd
from sqlalchemy.types import Integer, String, Float
from pandasql import sqldf
from CONEXAO import create_connection_postgre
import DW_TOOLS as dwt

pd.set_option('display.max_columns', None)


def verify_fact_exists(conn):
    """
    Verifica se a fato existe

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    fact_venda -- dataframe da fato ou None;
    """
    try:
        fact_venda = dwt.read_table(
            conn=conn,
            schema='dw',
            table_name='f_venda',
            columns=[
                'sk_forma_pagamento',
                'sk_cliente',
                'sk_funcionario',
                'sk_loja',
                'sk_produto',
                'sk_dt_venda',
                'nu_nfc',
                'qtd_produto',
                'vl_preco_custo',
                'vl_percentual_lucro'],
            where='"sk_loja" > 0 LIMIT 10'
        )

        return fact_venda
    except:
        return None


def extract_fact_venda(conn):
    """
    Extrai a fato vendas

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    fact_venda -- dataframe da fato;
    """
    fact_venda = dwt.read_table(
        conn=conn,
        schema='DW',
        table_name='F_VENDA',
        columns=[
            'sk_forma_pagamento',
            'sk_cliente',
            'sk_funcionario',
            'sk_loja',
            'sk_produto',
            'sk_dt_venda',
            'nu_nfc',
            'qtd_produto',
            'vl_preco_custo',
            'vl_percentual_lucro'
        ]
    )

    return fact_venda


def extract_dim_forma_pagamento(conn):
    """
    Extrai a dimensão forma pagamento

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_forma_pagamento -- dataframe da dim_forma_pagamento;
    """
    dim_forma_pagamento = dwt.read_table(
        conn=conn,
        schema='dw',
        table_name='d_forma_pagamento',
        columns=["sk_forma_pagamento", "cd_forma_pagamento"]
    )

    return dim_forma_pagamento


def extract_dim_cliente(conn):
    """
    Extrai a dimensão cliente

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_cliente -- dataframe da dim_cliente;
    """
    dim_cliente = dwt.read_table(
        conn=conn,
        schema='dw',
        table_name='d_cliente',
        columns=["sk_cliente", "cd_cliente"]
    )

    return dim_cliente


def extract_dim_funcionario(conn):
    """
    Extrai a dimensão funcionario

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_funcionario -- dataframe da dim_funcionario;
    """
    dim_funcionario = dwt.read_table(
        conn=conn,
        schema='dw',
        table_name='d_funcionario',
        columns=["sk_funcionario", "cd_funcionario"]
    )

    return dim_funcionario


def extract_dim_data(conn):
    """
    Extrai a dimensão data

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_data -- dataframe da dim_data;
    """
    dim_data = (
        dwt.read_table(
            conn=conn,
            schema='dw',
            table_name='d_data',
            columns=['sk_data', 'dt_referencia']).
            assign(
            dt_referencia=lambda x:
            pd.to_datetime(
                x.dt_referencia,
                format='%Y-%m-%d %H:%M:%S')).
            assign(
            dt_referencia=lambda x: x.dt_referencia.astype(str)).
            assign(
            dt_referencia=lambda x: x.dt_referencia.apply(
                lambda y: y[:13])
        )
    )
    return dim_data


def extract_dim_produto(conn):
    """
    Extrai a dimensão dim_produto

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_produto -- dataframe da dim_produto;
    """
    dim_produto = (
        dwt.read_table(
            conn=conn,
            schema='dw',
            table_name='d_produto',
            columns=["sk_produto", "cd_produto",
                     "vl_preco_custo", "vl_percentual_lucro",
                     "dt_inicio", "dt_fim", "fl_ativo"]).
        assign(
            dt_inicio=lambda x:
            pd.to_datetime(
                x.dt_inicio,
                format='%Y-%m-%d %H:%M:%S'),
            dt_fim=lambda x:
            pd.to_datetime(
                x.dt_fim,
                format='%Y-%m-%d %H:%M:%S'))
    )

    return dim_produto


def extract_dim_loja(conn):
    """
    Extrai a dimensão loja

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    dim_produto -- dataframe da dim_produto;
    """
    dim_loja = (
        dwt.read_table(
            conn=conn,
            schema='dw',
            table_name='d_loja',
            columns=[
                "sk_loja",
                "cd_loja",
                "dt_inicio",
                "dt_fim",
                "fl_ativo"]).
        assign(
            dt_inicio=lambda x:
            pd.to_datetime(
                x.dt_inicio,
                format='%Y-%m-%d %H:%M:%S'),
            dt_fim=lambda x:
            pd.to_datetime(
                x.dt_fim,
                format='%Y-%m-%d %H:%M:%S'))

    )

    return dim_loja


def extract_stage_venda(conn):
    """
    Extrai a stage venda e item venda

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stage_venda -- dataframe da stage_venda;
    """
    stage_venda = (
        dwt.read_table(
            conn=conn,
            schema='stage',
            table_name='stg_venda',
            columns=['id_venda', 'id_pagamento', 'id_cliente',
                     'id_func', 'id_loja', 'nfc', 'data_venda']).
        assign(
            data_venda=lambda x: pd.to_datetime(
                x.data_venda,
                format='%Y-%m-%d %H:%M:%S')).
        assign(
            data_referencia=lambda x: x.data_venda.astype(str)).
        assign(
            data_referencia=lambda x: x.data_referencia.apply(
                lambda y: y[:13]),
            id_venda=lambda x: x.id_venda.astype('int64'),
            id_pagamento=lambda x: x.id_pagamento.astype('int64'),
            id_cliente=lambda x: x.id_cliente.astype('int64'),
            id_func=lambda x: x.id_func.astype('int64'),
            id_loja=lambda x: x.id_loja.astype('int64')
        ))

    stage_item_venda = (
        dwt.read_table(
            conn=conn,
            schema='stage',
            table_name='stg_item_venda',
            columns=[
                'id_venda',
                'id_produto',
                'qtd_produto']).
        assign(
            id_venda=lambda x: x.id_venda.astype('int64')))

    stg_venda = (
        stage_venda.
        pipe(
            pd.merge,
            right=stage_item_venda,
            left_on="id_venda",
            right_on="id_venda",
            suffixes=["_11", "_12"])
    )

    return stg_venda


def extract_new_values(conn):
    """
    Extrai novos registros encontrados na stage venda

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stage_venda -- dataframe da stage_venda;
    """
    stg_venda = extract_stage_venda(conn)

    fact_venda = extract_fact_venda(conn)

    new_values = (
        sqldf('\
                SELECT\
                stg.id_venda,\
                stg.id_pagamento,\
                stg.id_cliente,\
                stg.id_func,\
                stg.id_loja,\
                stg.nfc,\
                stg.data_venda,\
                stg.data_referencia,\
                stg.id_produto,\
                stg.qtd_produto\
                FROM stg_venda stg\
                LEFT JOIN fact_venda fact\
                ON fact.nu_nfc = stg.nfc\
                WHERE fact.nu_nfc IS NULL;'
              )
    )

    if len(new_values) > 0:
        dim_forma_pagamento = extract_dim_forma_pagamento(conn)
        dim_cliente = extract_dim_cliente(conn)
        dim_funcionario = extract_dim_funcionario(conn)
        dim_loja = extract_dim_loja(conn)
        dim_produto = extract_dim_produto(conn)
        dim_data = extract_dim_data(conn)

        fact_merged_dimensions = (
            new_values.
            pipe(
                dwt.merge_input,
                right=dim_data,
                left_on="data_referencia",
                right_on="dt_referencia",
                suff=['_16', '_17'],
                surrogate_key='sk_data').
            pipe(
                dwt.merge_input,
                right=dim_forma_pagamento,
                left_on="id_pagamento",
                right_on="cd_forma_pagamento",
                suff=["_01", "_02"],
                surrogate_key="sk_forma_pagamento").
            pipe(
                dwt.merge_input,
                right=dim_cliente,
                left_on="id_cliente",
                right_on="cd_cliente",
                suff=["_03", "_04"],
                surrogate_key="sk_cliente").
            pipe(
                dwt.merge_input,
                right=dim_funcionario,
                left_on="id_func",
                right_on="cd_funcionario",
                suff=["_05", "_06"],
                surrogate_key="sk_funcionario")
        )

    merge_with_produto = (
        sqldf('\
            SELECT\
            fmd.sk_forma_pagamento,\
            fmd.sk_cliente,\
            fmd.sk_funcionario,\
            fmd.sk_data,\
            fmd.data_venda,\
            fmd.qtd_produto,\
            fmd.nfc,\
            fmd.id_loja,\
            p.sk_produto,\
            p.vl_preco_custo,\
            p.vl_percentual_lucro\
            FROM fact_merged_dimensions fmd\
            LEFT JOIN dim_produto p\
            ON fmd.id_produto = p.cd_produto\
            WHERE p.cd_produto = fmd.id_produto AND p.fl_ativo = 1;'
              )
    )

    merge_with_loja = (
        sqldf(f'\
            SELECT\
            mwp.sk_forma_pagamento,\
            mwp.sk_cliente,\
            mwp.sk_funcionario,\
            mwp.sk_data,\
            mwp.data_venda,\
            mwp.qtd_produto,\
            mwp.nfc,\
            mwp.sk_produto,\
            mwp.vl_preco_custo,\
            mwp.vl_percentual_lucro,\
            l.sk_loja\
            FROM merge_with_produto mwp\
            LEFT JOIN dim_loja l ON mwp.id_loja = l.cd_loja\
            WHERE l.cd_loja = mwp.id_loja AND l.fl_ativo = 1;')
    )

    return merge_with_loja


def extract_new_venda(conn):
    """
    Extrai todas as tabelas necessárias para gerar a fato venda

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor DW;

    return:
    stg_cliente_endereco -- pandas.Dataframe;
    """
    stg_venda = extract_stage_venda(conn)
    dim_forma_pagamento = extract_dim_forma_pagamento(conn)
    dim_cliente = extract_dim_cliente(conn)
    dim_funcionario = extract_dim_funcionario(conn)
    dim_loja = extract_dim_loja(conn)
    dim_produto = extract_dim_produto(conn)
    dim_data = extract_dim_data(conn)

    stage_merged_dimensions = (
        stg_venda.
        pipe(
            dwt.merge_input,
            right=dim_data,
            left_on="data_referencia",
            right_on="dt_referencia",
            suff=['_16', '_17'],
            surrogate_key='sk_data').
        pipe(
            dwt.merge_input,
            right=dim_forma_pagamento,
            left_on="id_pagamento",
            right_on="cd_forma_pagamento",
            suff=["_01", "_02"],
            surrogate_key="sk_forma_pagamento").
        pipe(
            dwt.merge_input,
            right=dim_cliente,
            left_on="id_cliente",
            right_on="cd_cliente",
            suff=["_03", "_04"],
            surrogate_key="sk_cliente").
        pipe(
            dwt.merge_input,
            right=dim_funcionario,
            left_on="id_func",
            right_on="cd_funcionario",
            suff=["_05", "_06"],
            surrogate_key="sk_funcionario")

    )

    merge_with_produto = (
        sqldf('\
                SELECT\
                smd.sk_forma_pagamento,\
                smd.sk_cliente,\
                smd.sk_funcionario,\
                smd.sk_data,\
                smd.data_venda,\
                smd.qtd_produto,\
                smd.nfc,\
                smd.id_loja,\
                p.sk_produto,\
                p.vl_preco_custo,\
                p.vl_percentual_lucro\
                FROM stage_merged_dimensions smd\
                INNER JOIN dim_produto p\
                ON smd.id_produto = p.cd_produto\
                WHERE (p.dt_inicio <= smd.data_venda < p.dt_fim)\
                OR (p.dt_inicio <= smd.data_venda AND p.dt_fim IS NULL);')
    )

    merge_with_loja = (
        sqldf('\
                SELECT\
                mwp.sk_forma_pagamento,\
                mwp.sk_cliente,\
                mwp.sk_funcionario,\
                mwp.sk_data,\
                mwp.data_venda,\
                mwp.qtd_produto,\
                mwp.nfc,\
                mwp.sk_produto,\
                mwp.vl_preco_custo,\
                mwp.vl_percentual_lucro,\
                l.sk_loja\
                FROM merge_with_produto mwp\
                INNER JOIN dim_loja l\
                ON mwp.id_loja = l.cd_loja\
                WHERE (l.dt_inicio <= mwp.data_venda < l.dt_fim)\
                OR (l.dt_inicio <= mwp.data_venda AND l.dt_fim IS NULL);')
    )

    return merge_with_loja


def treat_fact_venda(stg_venda):
    """
    Faz o tratamento dos dados extraidos das stages

    parâmetros:
    stg_venda -- pandas.Dataframe;

    return:
    fact_venda -- pandas.Dataframe;
    """
    columns_names = {
        "nfc": "nu_nfc",
        "sk_data": "sk_dt_venda"
    }

    columns_select = [
        'sk_forma_pagamento',
        'sk_cliente',
        'sk_funcionario',
        'sk_loja',
        'sk_produto',
        'sk_dt_venda',
        'nu_nfc',
        'qtd_produto',
        'vl_preco_custo',
        'vl_percentual_lucro'
    ]

    fact_venda = (
        stg_venda.
        rename(columns=columns_names).
        filter(columns_select)
    )

    return fact_venda


def treat_missing_data(fact_venda):
    """
    Faz o insert dos valores de data missing na fato venda

    parâmetros:
    fact_venda -- pandas.Dataframe;

    return:
    fact_venda -- pandas.Dataframe;
    """
    fact_venda = (
        pd.DataFrame([
            [-1, -1, -1, -1, -1, -1, -1, -1, -1, -1],
            [-2, -2, -2, -2, -2, -2, -2, -2, -2, -2],
            [-3, -3, -3, -3, -3, -3, -3, -3, -3, -3]
        ], columns=fact_venda.columns).append(fact_venda)
    )

    return fact_venda


def load_fact_venda(fact_venda, conn, action):
    """
    Faz a carga da fato venda no DW.

    parâmetros:
    fact_venda -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    action -- if_exists (append, replace...)
    """
    data_type = {
        "sk_forma_pagamento": Integer(),
        "sk_cliente": Integer(),
        "sk_funcionario": Integer(),
        "sk_loja": Integer(),
        "sk_dt_venda": Integer(),
        "sk_produto": Integer(),
        "nu_nfc": String(),
        "qtd_produto": Integer(),
        "vl_preco_custo": Float(),
        "vl_percentual_lucro": Float()
    }
    (
        fact_venda.
        to_sql(
            con=conn,
            name='f_venda',
            schema="dw",
            if_exists=action,
            chunksize=100,
            index=False,
            dtype=data_type
        )
    )


def run_fact_venda(conn):
    """
    Executa o pipeline da fato venda.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    fact_venda = verify_fact_exists(conn)
    if fact_venda is None:
        (
            extract_new_venda(conn).
            pipe(treat_fact_venda).
            pipe(treat_missing_data).
            pipe(load_fact_venda, conn=conn, action='replace')
        )
    else:
        (
            extract_new_values(conn).
            pipe(treat_fact_venda).
            pipe(load_fact_venda, conn=conn, action='append')
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
    run_fact_venda(conn_dw)
    print(f'exec time = {t.time() - start}')
