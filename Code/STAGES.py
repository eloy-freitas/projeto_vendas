import time
import DW_TOOLS as dwt
from CONEXAO import create_connection_postgre


def create_stg_venda(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='VENDA',
        stg_name='STG_VENDA',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_VENDA: {exec_time:.4f}')


def create_stg_item_venda(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='ITEM_VENDA',
        stg_name='STG_ITEM_VENDA',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_ITEM_VENDA: {exec_time:.4f}')


def create_stg_loja(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='LOJA',
        stg_name='STG_LOJA',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_LOJA: {exec_time:.4f}')


def create_stg_produto(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='PRODUTO',
        stg_name='STG_PRODUTO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_PRODUTO: {exec_time:.4f}')


def create_stg_forma_pagamento(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='FORMA_PAGAMENTO',
        stg_name='STG_FORMA_PAGAMENTO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_FORMA_PAGAMENTO {exec_time:.4f}')


def create_stg_cliente(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='CLIENTE',
        stg_name='STG_CLIENTE',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_CLIENTE: {exec_time:.4f}')


def create_stg_funcionario(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='FUNCIONARIO',
        stg_name='STG_FUNCIONARIO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_FUNCIONARIO: {exec_time:.4f}')


def create_stg_endereco(conn_dw):
    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='ENDERECO',
        stg_name='STG_ENDERECO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STG_ENDERECO: {exec_time:.4f}')


if __name__ == '__main__':
    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    create_stg_venda(conn_dw)
    create_stg_funcionario(conn_dw)
    create_stg_forma_pagamento(conn_dw)
    create_stg_item_venda(conn_dw)
    create_stg_produto(conn_dw)
    create_stg_loja(conn_dw)
    create_stg_cliente(conn_dw)
    create_stg_endereco(conn_dw)