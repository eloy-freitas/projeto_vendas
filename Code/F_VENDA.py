import time as t
import pandas as pd
from sqlalchemy.types import Integer, String, Float, DateTime
from pandasql import sqldf
from Code.CONEXAO import create_connection_postgre
import Code.DW_TOOLS as dwt

pd.set_option('display.max_columns', None)


def extract_stg_venda(conn, load_date):
    """
    Extrai a stage venda e item venda

    :parameter:
        conn -- sqlalchemy.engine;

    :return:
        stg_venda -- pandas.Dataframe;
    """
    if load_date is None:
        stg_venda = (
            dwt.read_table(
                conn=conn,
                schema="stage",
                table_name="stg_venda",
                columns=["id_venda", "id_pagamento", "id_cliente",
                         "id_func", "id_loja", "nfc", "data_venda"]).
            assign(
                data_venda_2=lambda x: x.data_venda.astype("datetime64").dt.floor("h")
            )
        )
    else:
        stg_venda = (
            dwt.read_table(
                conn=conn,
                schema="stage",
                table_name="stg_venda",
                columns=["id_venda", "id_pagamento", "id_cliente",
                         "id_func", "id_loja", "nfc", "data_venda"],
                where=f"data_venda > '{load_date}'").
            assign(
                data_venda_2=lambda x: x.data_venda.astype("datetime64").dt.floor("h")
            )
        )

    return stg_venda


def extract_fact_venda(conn, load_date):
    """
    Extrai os registros encontrados na stage venda e relaciona com as outrad dimensões

    :parameter:
        conn -- sqlalchemy.engine;

    :return:
        tbl_venda -- pandas.Dataframe;
    """
    stg_venda = extract_stg_venda(conn, load_date)

    if stg_venda.shape[0] != 0:

        stage_item_venda = dwt.read_table(
            conn=conn,
            schema="stage",
            table_name="stg_item_venda",
            columns=["id_venda", "id_produto", "qtd_produto"]
        )

        dim_forma_pagamento = dwt.read_table(
            conn=conn,
            schema="dw",
            table_name="d_forma_pagamento",
            columns=["sk_forma_pagamento", "cd_forma_pagamento"]
        )

        dim_cliente = dwt.read_table(
            conn=conn,
            schema="dw",
            table_name="d_cliente",
            columns=["sk_cliente", "cd_cliente"]
        )

        dim_funcionario = dwt.read_table(
            conn=conn,
            schema="dw",
            table_name="d_funcionario",
            columns=["sk_funcionario", "cd_funcionario"]
        )

        dim_data = dwt.read_table(
            conn=conn,
            schema="dw",
            table_name="d_data",
            columns=["sk_data", "dt_referencia"]
        )

        dim_produto = dwt.read_table(
            conn=conn,
            schema="dw",
            table_name="d_produto",
            columns=["sk_produto", "cd_produto","vl_preco_custo", "vl_percentual_lucro",
                     "dt_inicio", "dt_fim", "fl_ativo"]
        )

        dim_loja = dwt.read_table(
            conn=conn,
            schema="dw",
            table_name="d_loja",
            columns=["sk_loja", "cd_loja", "dt_inicio", "dt_fim", "fl_ativo"]
        )

        tbl_venda_temp = (
            stg_venda.
            pipe(
                pd.merge,
                right=stage_item_venda,
                left_on="id_venda",
                right_on="id_venda",
                suffixes=["_11", "_12"]).
            pipe(
                dwt.merge_input,
                right=dim_data,
                left_on="data_venda_2",
                right_on="dt_referencia",
                suff=["_16", "_17"],
                surrogate_key="sk_data").
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

        query = """
            SELECT
                v.id_venda,
                v.sk_forma_pagamento,
                v.sk_cliente,
                v.sk_funcionario,
                v.sk_data,
                v.data_venda,
                v.qtd_produto,
                v.nfc,
                v.id_loja,
                p.sk_produto,
                p.vl_preco_custo,
                p.vl_percentual_lucro,
                l.sk_loja
            FROM venda v
            LEFT JOIN dw.d_produto p
            ON (v.id_produto = p.cd_produto
                            AND date(v.data_venda) >= date(p.dt_inicio)
                            AND (date(v.data_venda) < date(p.dt_fim) 
                                OR p.dt_fim IS NULL)
                     )
            LEFT JOIN dw.d_loja l
            ON (v.id_loja = l.cd_loja
                    AND date(v.data_venda) >= date(l.dt_inicio) 
                    AND (date(v.data_venda) < date(l.dt_fim)
                        OR l.dt_fim IS NULL)
            )
        """
        tbl_venda = sqldf(query, {'venda': tbl_venda_temp}, conn.url)
    else:
        tbl_venda=stg_venda

    return tbl_venda


def treat_fact_venda(tbl_venda):
    """
    Faz o tratamento dos dados extraidos das stages

    :parameter:
        tbl_venda -- pandas.Dataframe;

    :return:
        fact_venda -- pandas.Dataframe;
    """
    columns_names = {
        'id_venda': 'cd_venda',
        'nfc': 'nu_nfc',
        'data_venda': 'dt_venda',
        'qtd_produto': 'qtd_produto',
        'sk_data': 'sk_data_venda'
    }

    columns_select = [
        'sk_forma_pagamento',
        'sk_cliente',
        'sk_funcionario',
        'sk_loja',
        'sk_produto',
        'sk_dt_venda',
        'cd_venda',
        'nu_nfc',
        'qtd_produto',
        'vl_preco_custo',
        'vl_percentual_lucro',
        'dt_venda',
        'dt_carga'
    ]

    fact_venda = (
        tbl_venda.
        rename(columns=columns_names).
        filter(columns_select).
        assign(
            sk_produto=lambda x: x["sk_produto"].fillna(value=-3).astype("Int64"),
            sk_loja=lambda x: x["sk_loja"].fillna(value=-3).astype("Int64"),
            dt_carga=pd.to_datetime('today', format='%Y-%m-%d')
        )
    )

    return fact_venda


def load_fact_venda(fact_venda, conn):
    """
    Faz a carga da fato venda no DW.

    :parameter:
        fact_venda -- pandas.Dataframe;
        conn -- sqlalchemy.engine;
    """
    data_type = {
        "cd_venda":Integer(),
        "dt_venda":DateTime(),
        "sk_forma_pagamento": Integer(),
        "sk_cliente": Integer(),
        "sk_funcionario": Integer(),
        "sk_loja": Integer(),
        "sk_dt_venda": Integer(),
        "sk_produto": Integer(),
        "nu_nfc": String(),
        "qtd_produto": Integer(),
        "vl_preco_custo": Float(),
        "vl_percentual_lucro": Float(),
        "dt_carga": DateTime()
    }
    (
        fact_venda.
        to_sql(
            con=conn,
            name='f_venda',
            schema="dw",
            if_exists='append',
            chunksize=100,
            index=False,
            dtype=data_type
        )
    )


def run_fact_venda(conn):
    """
    Executa o pipeline da fato venda.

    :parameter:
        conn -- sqlalchemy.engine;
    """
    if dwt.verify_table_exists(conn=conn, schema='dw', table='f_venda'):
        result = conn.execute('SELECT MAX(dt_venda) FROM dw.f_venda')
        date_max = result.fetchone()[0]
        load_date = pd.to_datetime(date_max, format="%Y-%m-%d %H:%M:%S")
    else:
        load_date = None

    tbl_fact = extract_fact_venda(conn, load_date)
    if tbl_fact.shape[0] != 0:
        (
            treat_fact_venda(tbl_venda=tbl_fact).
            pipe(load_fact_venda, conn=conn)
        )


if __name__ == '__main__':
    conn = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )
    start = t.time()
    run_fact_venda(conn)
    print(f'exec time = {t.time() - start}')
