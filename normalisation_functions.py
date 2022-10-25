import pandas as pd


def column_to_list_generator(df, column_name):
    column_df = pd.DataFrame(df[column_name].dropna(axis=0).unique())
    column_list = sorted(column_df[0].tolist())
    return column_list


def df_id_generator(list_values, id_suffix, table_keyword: str):
    id_list = []
    i = 1
    table_keyword = table_keyword.lower()
    headers = [f'{table_keyword}_id', f'{table_keyword}_name']
    for value in list_values:
        id_list.append([f'{id_suffix}{i}', value])
        i += 1
    df = pd.DataFrame(id_list, columns=headers)
    return df




