import pandas as pd



def read_file(path, delimiter):  # leitura do arquivo
    return pd.read_csv(
        path,
        sep=delimiter,
        error_bad_lines=False,
        low_memory=True,
        encoding='utf8'
    )


# função para inserir dados no banco de dados
def insert_data(data, connection, table_name, schema_name, action):
    data.to_sql(
            name=table_name,
            con=connection,
            schema=schema_name,
            if_exists=action,
            index=False,
            chunksize=100
    )


def merge_input(left, right, left_on, right_on, surrogate_key, suff):
    dict_na = right.query(f"{surrogate_key} == -3").to_dict('index')

    df = (
        left.
            merge(right, how="left", left_on=left_on, right_on=right_on, suffixes=suff).
            fillna(dict_na[list(dict_na)[0]])
    )

    return df


def get_data_from_database(conn_input, sql_query):
    return pd.read_sql_query(
        sql=sql_query,
        con=conn_input,
        coerce_float=True
    )


# função que prenche uma tabela com seguimentos do dataframe
def fill_table(data_frame,
               connection,
               table,
               frame_size,
               use_index,
               index_name):
    try:
        print('filling table {}...'.format(table))
        size = data_frame.shape[0]
        frame = int(size / frame_size)
        for i in range(0, size + 1, frame):
            data = data_frame.loc[i:i + frame - 1]
            insert_data(data,
                        connection,
                        table,
                        'append',
                        use_index,
                        index_name)
    except Exception as e:
        print(e)


def resume_dataframe(dataframe):
    print('\ncaracterísticas do dataframe:\n')
    print('colunas:\n', dataframe.columns)
    print('tipo de dados das colunas:\n', dataframe.dtypes)
    print('dimensões do dataframe:\n', dataframe.shape)
    print('há dados missing:\n', dataframe.isna().any().any())
    print('quantidadee de dados missing:\n', dataframe.isna().sum().sum())
    print('há dados nulos:\n', dataframe.isnull().any().any())
    print('quantidadee de dados nulos:\n', dataframe.isnull().sum().sum())
    print('descrição estatística:\n', dataframe.describe())
