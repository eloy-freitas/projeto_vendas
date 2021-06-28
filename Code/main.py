import time
from CONEXAO import create_connection_postgre
from STAGES import create_stage_from_db
import DW_TOOLS as dwt


# Press the green button in the gutter to run the script.
if __name__ == '__main__':

    conn_dw = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='FORMA_PAGAMENTO',
        stg_name='STAGE_FORMA_PAGAMENTO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_FORMA_PAGAMENTO {exec_time:.4f}')

    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='CLIENTE',
        stg_name='STAGE_CLIENTE',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_CLIENTE: {exec_time:.4f}')

    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='FUNCIONARIO',
        stg_name='STAGE_FUNCIONARIO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_FUNCIONARIO: {exec_time:.4f}')

    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='LOJA',
        stg_name='STAGE_LOJA',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_LOJA: {exec_time:.4f}')

    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='ENDERECO',
        stg_name='STAGE_ENDERECO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_ENDERECO: {exec_time:.4f}')

    tart = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='PRODUTO',
        stg_name='STAGE_PRODUTO',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_PRODUTO: {exec_time:.4f}')

    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='VENDA',
        stg_name='STAGE_VENDA',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_VENDA: {exec_time:.4f}')

    start = time.time()
    dwt.create_stage(
        conn_input=conn_dw,
        conn_output=conn_dw,
        schema_in='public',
        table='ITEM_VENDA',
        stg_name='STAGE_ITEM_VENDA',
        tbl_exists='replace'

    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_ITEM_VENDA: {exec_time:.4f}')


