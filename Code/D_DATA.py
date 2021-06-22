import pandas as pd
import time as t
from CONEXAO import create_connection_postgre
from tools import insert_data


def treat_dim_data():
    select_columns = [
        "DT_REFERENCIA"
    ]

    data = pd.date_range(
                start='2020-01-01',
                end='2023-01-01',
                freq='H')

    dim_data = pd.DataFrame(
        data=data,
        columns=select_columns).assign(
        DT_ANO=lambda x: x.DT_REFERENCIA.dt.year,
        DT_MES=lambda x: x.DT_REFERENCIA.dt.month,
        DT_TRIMESTE=lambda x: x.DT_REFERENCIA.dt.quarter,
        DT_DIA=lambda x: x.DT_REFERENCIA.dt.day,
        DT_SEMANA=lambda x: x.DT_REFERENCIA.dt.isocalendar().week,
        DS_DIA_SEMANA=lambda x: x.DT_REFERENCIA.dt.day_name(),
        DT_HORA=lambda x: x.DT_REFERENCIA.dt.hour,
        DS_TURNO=lambda x: x.DT_HORA.apply(
            lambda y:
            "Manhã" if 6 <= y < 12 else
            "Tarde" if 12 <= y < 18 else
            "Noite" if 18 <= y < 24 else
            "Madrugada"),
        DT_REFERENCIA=lambda x: x.DT_REFERENCIA.dt.date
    )

    dim_data.insert(0, 'SK_DATA', range(1, 1 + len(dim_data)))

    dim_data = (
        pd.DataFrame([
            [-1, -1, -1, -1, -1, -1, -1, "Não informado", -1, "Não informado"],
            [-2, -2, -2, -2, -2, -2, -2, "Não aplicável", -2, "Não aplicável"],
            [-3, -3, -3, -3, -3, -3, -3, "Desconhecido", -3, "Desconhecido"]
        ], columns=dim_data.columns).append(dim_data)
    )

    return dim_data


def load_dim_data(dim_data, conn):
    dim_data.to_sql(
        con=conn,
        name='D_DATA',
        schema='DW',
        if_exists='replace',
        index=False,
        chunksize=100
    )


def run_dim_data(conn):
    (
        treat_dim_data().
            pipe(load_dim_data, conn=conn)
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
    run_dim_data(conn_dw)
    exec_time = t.time() - start
    print(f"exec_time = {exec_time}")
