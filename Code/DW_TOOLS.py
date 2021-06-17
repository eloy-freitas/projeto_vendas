import pandas as pd


def merge_input(left, right, left_on, right_on, surrogate_key, suff):
    dict_na = right.query(f"{surrogate_key} == -3").to_dict('index')

    df = (
        left.
            merge(right, how="left", left_on=left_on, right_on=right_on, suffixes=suff).
            fillna(dict_na[list(dict_na)[0]])
    )

    return df


def create_stage(path, delimiter, conn_output, stg_name, where=None):
    (
        pd.read_csv(path,
                    sep=delimiter,
                    low_memory=False).to_sql(name=stg_name,
                                             con=conn_output,
                                             schema="STAGE",
                                             if_exists="replace",
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
