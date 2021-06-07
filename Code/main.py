import time
from CONEXAO import create_connection_postgre
from STAGES import create_stage_from_db


# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    conn_in = create_connection_postgre(
        server="192.168.3.2",
        database="db_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    conn_out = create_connection_postgre(
        server="192.168.3.2",
        database="projeto_dw_vendas",
        username="itix",
        password="itix123",
        port="5432"
    )

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "FORMA_PAGAMENTO";',
        table='STAGE_FORMA_PAGAMENTO'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_FORMA_PAGAMENTO {exec_time:.4f}')

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "CLIENTE";',
        table='STAGE_CLIENTE'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_CLIENTE: {exec_time:.4f}')

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "FUNCIONARIO";',
        table='STAGE_FUNCIONARIO'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_FUNCIONARIO: {exec_time:.4f}')

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "LOJA";',
        table='STAGE_LOJA'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_LOJA: {exec_time:.4f}')

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "ENDERECO";',
        table='STAGE_ENDERECO'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_ENDERECO: {exec_time:.4f}')

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "PRODUTO";',
        table='STAGE_PRODUTO'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_PRODUTO: {exec_time:.4f}')

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "VENDA";',
        table='STAGE_VENDA'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_VENDA: {exec_time:.4f}')

    start = time.time()
    create_stage_from_db(
        conn_in=conn_in,
        conn_out=conn_out,
        query=f'select * from "ITEM_VENDA";',
        table='STAGE_ITEM_VENDA'
    )
    exec_time = time.time() - start
    print(f'tempo de execução da stage STAGE_ITEM_VENDA: {exec_time:.4f}')


