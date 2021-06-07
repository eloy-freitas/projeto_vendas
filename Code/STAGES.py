from tools import get_data_from_database, insert_data


def create_stage_from_db(conn_in, conn_out, query, table):
    df = get_data_from_database(
        conn_input=conn_in,
        sql_query=query
    )

    insert_data(
        data=df,
        connection=conn_out,
        table_name=table,
        schema_name='STAGES',
        action='replace'
    )
