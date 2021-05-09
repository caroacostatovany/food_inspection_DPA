import pandas as pd
from aequitas.preprocessing import preprocess_input_df

from src.utils.general import get_db_conn_psycopg
from src.utils.constants import CREDENCIALES


def obtain_aequitas_dataframe():
    query = """
                select * 
                from semantic.aequitas;
            """

    conn = get_db_conn_psycopg(CREDENCIALES)
    aequitas_df = pd.read_sql(query, conn)

    aequitas_df, _ = preprocess_input_df(aequitas_df)

    return aequitas_df


def obtain_metricas_sesgo_dataframe():
    query = """
                select * 
                from results.sesgo;
            """

    conn = get_db_conn_psycopg(CREDENCIALES)
    df = pd.read_sql(query, conn)

    return df
