"""
Módulo de feature engineering
"""

import pandas as pd
import pickle
import logging

from sklearn.preprocessing import OneHotEncoder
from sklearn.model_selection import train_test_split, GridSearchCV, TimeSeriesSplit
from sklearn.compose import ColumnTransformer
from sklearn.ensemble import RandomForestClassifier
from sklearn.feature_selection import VarianceThreshold
from sklearn import metrics

from src.utils.constants import L
from src.etl.ingesta_almacenamiento import get_s3_resource

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
         - df: dataframe
    * Return:
         - df: dataframe con las nuevas columnas
    ==========
    Ejemplo:
        >> food_inspection_df = aggregate_num_violations(food_inspection_df)
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
    """
    Eliminar renglones con nan
    ==========
    * Args:
         - df: dataframe con nan's
    * Return:
         - df: dataframe sin nan's
    ==========
    Ejemplo:
        >> food_inspection_df = remove_nan_rows(food_inspection_df)
    """

    df = df.dropna()
    return df


def feature_generation(df):
    """
    Generación de variables a formato deseable para análisis de datos y futuro modelado
    ==========
    * Args:
         - df: dataframe
    * Return:
         - df: dataframe con variables generadas
    ==========
    Ejemplo:
        >> food_inspection_df = feature_generation(food_inspection_df)
    """

    df = transformate_and_aggregate_dates(df)
    df = aggregate_num_violations(df)
    df = remove_non_useful_columns(df)

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
    del df
    X = X.todense()
    df = pd.DataFrame(X, columns=col_trans.get_feature_names())
    del X
    df['label'] = y

    logging.info("Feature engineering succesfully...")
    return df


def feature_selection(df):
    """
    Selección de variables, partición de datos en entrenamiento y test con 70% para entrenamiento y 30% de prueba
    ==========
    * Args:
         - df: dataframe
    * Return:
         - X_train, X_test, y_train, y_test:  datos para entrenamiento y prueba
    ==========
    Ejemplo:
        >> X_train, X_test, y_train, y_test = feature_selection(food_inspection_df)
    """
    # Separación en train y test manualmente para no hacer data leaking
    lim = round(df.shape[0] * .70)  # 70% de train
    X_train, X_test = df[:lim].drop('label', axis=1), df[lim:].drop('label', axis=1)
    y_train, y_test = df[['label']][:lim], df[['label']][lim:]

    return X_train, X_test, y_train, y_test

def guardar_feature_engineering(bucket_name, file_to_upload, data, credenciales):
    """
    Guardar los datos dentro del bucket en el path especificado
    Inputs:
    bucket_name:  bucket s3
    file_to_upload(string): nombre y ruta del archivo a guardar
    data(json): objeto json con los datos
    Outputs:
    None
    """

    # Obtener bucket
    s3 = get_s3_resource(credenciales)

    # Cambiar datos de formato json a objetos binario
    pickle_dump = pickle.dumps(data)

    # Guardar los datos (pickle) en el bucket y ruta específica
    s3.put_object(Bucket=bucket_name, Key=file_to_upload, Body=pickle_dump)
    logging.info("pkl guardado exitosamente.")