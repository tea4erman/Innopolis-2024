import pandas as pd
import numpy as np
import psycopg2

def query_db(
    query, 
    dbname = 'dvdrental', 
    user = 'postgres',
    host = 'localhost',
    port = '5432',
    password = '123'
    ):
    engine = psycopg2.connect(f"dbname={dbname} user={user} host={host} port={port} password={password}")
    df = pd.read_sql(query, con=engine)
    return df

def get_numeric_stats(df, columns_to_exclude = []):
    data_types = df.dtypes
    numeric_columns = data_types.loc[(data_types == int)|(data_types == float)].index.to_list()
    for col in columns_to_exclude:
        numeric_columns.remove(col)
    num_stats = df[numeric_columns].describe().loc[['min', 'mean', 'max']]
    return num_stats