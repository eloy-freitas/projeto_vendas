import pandas as pd
import time as t
from sqlalchemy.types import String, DateTime, Integer
from CONEXAO import create_connection_postgre


def treat_dim_data():
    """
    Gera os dados da dimensão data com base em um intervalo de datas

    return:
    dim_data -- pandas.DataFrame;
    """
    select_columns = [
        "dt_referencia"
    ]

    data = pd.date_range(
                start='2020-01-01',
                end='2023-01-01',
                freq='H')

    dim_data = (
        pd.DataFrame(
            data=data,
            columns=select_columns).
        assign(
            dt_ano=lambda x: x.dt_referencia.dt.year,
            dt_mes=lambda x: x.dt_referencia.dt.month,
            dt_trimestre=lambda x: x.dt_referencia.dt.quarter,
            dt_dia=lambda x: x.dt_referencia.dt.day,
            dt_semana=lambda x: x.dt_referencia.dt.isocalendar().week,
            ds_dia_semana=lambda x: x.dt_referencia.dt.day_name(),
            dt_hora=lambda x: x.dt_referencia.dt.hour,
            ds_turno=lambda x: x.dt_hora.apply(
                lambda y:
                "Manhã" if 6 <= y < 12 else
                "Tarde" if 12 <= y < 18 else
                "Noite" if 18 <= y < 24 else
                "Madrugada")
        )
    )

    dim_data.insert(0, 'sk_data', range(1, 1 + len(dim_data)))

    dim_data = (
        pd.DataFrame([
            [-1, None, -1, -1, -1, -1, -1, "Não informado", -1, "Não informado"],
            [-2, None, -2, -2, -2, -2, -2, "Não aplicável", -2, "Não aplicável"],
            [-3, None, -3, -3, -3, -3, -3, "Desconhecido", -3, "Desconhecido"]
        ], columns=dim_data.columns).append(dim_data)
    )

    return dim_data


def load_dim_data(dim_data, conn):
    """
    Faz a carga da dimensão data no DW.

    parâmetros:
    dim_cliente -- pandas.Dataframe;
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    data_types = {
        "sk_data": Integer(),
        "dt_referencia": DateTime(),
        "dt_ano": Integer(),
        "dt_mes": Integer(),
        "dt_trimestre": Integer(),
        "dt_dia": Integer(),
        "dt_semana": Integer(),
        "ds_dia_semana": String(),
        "dt_hora": Integer(),
        "ds_turno": String()
    }
    (
        dim_data.astype('string').
            to_sql(
            con=conn,
            name='d_data',
            schema='dw',
            if_exists='replace',
            index=False,
            chunksize=100,
            dtype=data_types
        )
    )


def run_dim_data(conn):
    """
    Executa o pipeline da dimensão data.

    parâmetros:
    conn -- conexão criada via SqlAlchemy com o servidor do DW;
    """
    (
        treat_dim_data().
            pipe(load_dim_data, conn=conn)
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
    run_dim_data(conn_dw)
    print(f'exec time = {t.time() - start}')