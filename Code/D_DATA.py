import pandas as pd
import time as t
from sqlalchemy.types import String, DateTime, Integer
from CONEXAO import create_connection_postgre


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
            "Madrugada")
    )

    dim_data.insert(0, 'SK_DATA', range(1, 1 + len(dim_data)))

    dim_data = (
        pd.DataFrame([
            [-1, None, -1, -1, -1, -1, -1, "Não informado", -1, "Não informado"],
            [-2, None, -2, -2, -2, -2, -2, "Não aplicável", -2, "Não aplicável"],
            [-3, None, -3, -3, -3, -3, -3, "Desconhecido", -3, "Desconhecido"]
        ], columns=dim_data.columns).append(dim_data)
    )

    return dim_data


def load_dim_data(dim_data, conn):
    data_types = {
        "SK_DATA": Integer(),
        "DT_REFERENCIA": DateTime(),
        "DT_ANO": Integer(),
        "DT_MES": Integer(),
        "DT_TRIMESTE": Integer(),
        "DT_DIA": Integer(),
        "DT_SEMANA": Integer(),
        "DS_DIA": String(),
        "DT_HORA": Integer(),
        "DS_TURNO": String()
    }
    (
        dim_data.astype('string').
            to_sql(
            con=conn,
            name='D_DATA',
            schema='DW',
            if_exists='replace',
            index=False,
            chunksize=100,
            dtype=data_types
        )
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
