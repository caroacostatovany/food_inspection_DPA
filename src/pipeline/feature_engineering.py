"""
Módulo de feature engineering
"""
import pandas as pd
import logging

from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, TimeSeriesSplit
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import VarianceThreshold
from sklearn import metrics

from src.utils.constants import L

logging.basicConfig(level=logging.INFO)


def transformate_and_aggregate_dates(df):
    """
    Cambia a tipo correcto fecha y separa el inspection date y
    calcula de acuerdo a la lista declara en L
    ['year', 'month', 'day', 'dayofweek', 'dayofyear', 'week', 'quarter']
    ==========
    * Args:
         - df
    * Return:
         - df: dataframe con las nuevas columnas
    ==========
    Ejemplo:
        >> food_inspection_df = transformate_and_aggregate_dates(L, food_inspection_df)
    """
    df['inspection_date'] = pd.to_datetime(df['inspection_date'])

    date_gen = (getattr(df['inspection_date'].dt, i).rename(i) for i in L)

    # concatenamos
    df = df.join(pd.concat(date_gen, axis=1))

    return df


def aggregate_num_violations(df):
    """
    Agrega el número de violaciones de acuerdo a las violaciones descritas en la columna "violations".
    Si esta en nulo, tiene 0 violaciones, independientemente del resultado o riesgo.
    ==========
    * Args:
         - lista: ejemplo >> ['year', 'month', 'day', 'dayofweek', 'dayofyear', 'week', 'quarter']
         - df
    * Return:
         - df: dataframe con las nuevas columnas
    ==========
    Ejemplo:
        >> food_inspection_df = transformate_and_aggregate_dates(L, food_inspection_df)
    """
    f = lambda x: len(x['violations'].split('|')) if type(x['violations']) == str else 0
    df["num_violations"] = df.apply(f, axis=1)

    return df


def remove_non_useful_columns(df):
    """
    Quitar las columnas no necesarias
    ==========
    * Args:
         - df: dataframe
    * Return:
         - df: dataframe
    ==========
    Ejemplo:
        >> food_inspection_df = remove_non_useful_columns(food_inspection_df)
    """
    # Vamos a quitar address para evitar un onehotencoder muy largo
    non_useful_cols = ['location', 'inspection_date', 'inspection_id', 'dba_name', 'aka_name', 'violations',
                       'address', 'results', 'license_']

    df = df.drop(non_useful_cols, axis=1)

    # quitamos los renglones que tienen datos vacíos en cualquier columna
    df = remove_nan_rows(df)

    return df


def remove_nan_rows(df):

    df = df.dropna()
    return df


def feature_generation(df):
    df = transformate_and_aggregate_dates(df)
    df = aggregate_num_violations(df)


    # Aplicamos OneHot Encoder para las categóricas
    transformers = [('one_hot', OneHotEncoder(), ['facility_type', 'risk',
                                                  'city', 'state', 'inspection_type'])]

    col_trans = ColumnTransformer(transformers, remainder="passthrough", n_jobs=-1)

    # Ordenaremos el dataframe temporalmente
    df = df.sort_values(by=["year", "month", "day"])

    X = col_trans.fit_transform(df.drop(columns="label"))
    y = df['label'].values.reshape(X.shape[0], )
    logging.info("Successfully transformation of the discrete variables.'")

    logging.info("Converting to dataframe...")
    X = X.todense()
    df = pd.DataFrame(X, columns=col_trans.get_feature_names())
    df['label'] = y

    return df


def feature_selection(df):

    # Separación en train y test manualmente para no hacer data leaking
    lim = round(df.shape[0] * .70)  # 70% de train
    X_train, X_test = df[:lim].drop('label', axis=1), df[lim:].drop('label', axis=1)
    y_train, y_test = df[['label']][:lim], df[['label']][lim:]

    return  X_train, X_test, y_train, y_test

