import pandas as pd
import sqlalchemy as sqla


def merge_input(left, right, left_on, right_on, surrogate_key, suff):
    dict_na = right.query(f"{surrogate_key} == -3").to_dict('index')

    df = (
        left.
        merge(right, how="left", left_on=left_on, right_on=right_on, suffixes=suff).
        fillna(dict_na[list(dict_na)[0]])
    )

    return df


def create_stage(conn_input, conn_output, schema_in, table, stg_name, tbl_exists):
    (
        read_table(conn=conn_input, schema=schema_in, table_name=table).
            to_sql(name=stg_name,
                   con=conn_output,
                   schema="stage",
                   if_exists=tbl_exists,
                   index=False)
    )


def dict_to_str(dict):
    str = list()
    for value in dict.keys():
        temp_str = f'{value}" AS "{dict.get(value)}'
        str.append(temp_str)

    return str


def concat_cols(str):
    if isinstance(str, dict):
        str = dict_to_str(str)

    return '", "'.join(str)


def read_table(conn, schema, table_name, columns=None, where=None, distinct=False):
    if distinct:
        distinct_clause = "DISTINCT"
    else:
        distinct_clause = ""

    if where is None:
        where_clause = ""
    else:
        where_clause = f"WHERE {where}"

    if columns is None:
        query = f'SELECT {distinct_clause} * FROM "{schema}"."{table_name}" {where_clause}'
    else:
        query = f'SELECT {distinct_clause} "{concat_cols(columns)}" FROM "{schema}"."{table_name}" {where_clause}'

    response = pd.read_sql_query(query, conn)

    return response


def find_max_sk(conn, schema, table, sk_name):
    query = (
        f'SELECT MAX({sk_name}) FROM {schema}.{table};'
    )
    return conn.execute(query).fetchone()[0] + 1


def verify_table_exists(conn, schema, table):
    if table in sqla.inspect(conn).get_table_names(schema=schema):
        return True
    return False
