"""
Módulo de preprocesamiento y limpieza de datos
"""
import pandas as pd
import logging

from geopy.geocoders import Nominatim

logging.basicConfig(level=logging.INFO)


def get_zipcode(df, geolocator, lat_field, lon_field):
    """
    Obtiene el código zip utilizando latitud y longitud

    ==========
    * Args:
         - df: dataframe
         - geolocator: objeto Nominatim de geocoder con http
         - lat_field: latitud
         - lon_field: longitud
    * Return:
         - location.raw['address']['postcode']: lista de los códigos zip
    ==========
    Ejemplo:
        >> zipcodes = zip_notnull_df.apply(get_zipcode, axis=1,
                                    geolocator=geolocator,
                                    lat_field='latitude', lon_field='longitude')
    """
    location = geolocator.reverse((df[lat_field], df[lon_field]))

    return location.raw['address']['postcode']


def transform_label(df):
    """
    Transforma la etiqueta en 1 y 0.
    > 1 para los resultados que fueron "Pass" y "Pass w/ conditions"
    > 0 para el resto

    ==========
    * Args:
         - df: dataframe
    * Return:
         - df: dataframe
    ==========
    Ejemplo:
        >> food_inspection_df = transform_label(food_inspection_df)
    """

    f = lambda s: 1 if (s['results'] == 'pass') or (s['results'] == 'pass w/ conditions') else 0
    df["label"] = df.apply(f, axis=1)

    return df


def df_to_lower_case(df):
    """
    Convierte a minúsculas todas las celdas de tipo str en un dataframe
    ==========
    * Args:
         - df: dataframe
    * Return:
         - df: dataframe en minúsculas
    ==========
    Ejemplo:
        >> food_inspection_df = df_to_lower_case(food_inspection_df)
    """

    df = df.applymap(lambda s: s.lower() if type(s) == str else s)

    return df


# No es la mejor forma pero ..mientras :3 (Lo siento)
def change_misspelled_chicago_city_names(df):
    """
    Cambia los nombres de Chicago y Olympia fields que pueden estar mal escritos.
    ==========
    * Args:
         - df: dataframe
    * Return:
         - df: dataframe
    ==========
    Ejemplo:
        >> food_inspection_df = change_misspelled_chicago_city_names(food_inspection_df)
    """
    lista_misspelled = ['cchicago', 'chicago.', 'chicagohicago', 'chicagochicago', '312chicago', 'chicago.',
                        'chcicago', 'chchicago', 'chicagoi']

    for name in lista_misspelled:
        df.loc[df['city'] == name, 'city'] = 'chicago'

    df.loc[df['city'] == 'oolympia fields', 'city'] = 'olympia fields'

    return df


def convert_nan(df):
    """
    Imputa los valores nulos de la siguiente forma:
    Para los valores nulos de:
    facility_type -> unkown_facility_type
    inspection_type -> unkown_inspection_type
    license_ -> unkown_license
    city -> unkown_city
    state -> unkown_state
    risk -> unkown_risk
    zip -> los obtiene de la ubicación con geocoder

    Corrige:
     > para los que tienen ciudad 'chicago' a estado 'il'
     > para el riesgo, mantiene sólo una palabra, lo que esta dentro del paréntesis

    ==========
    * Args:
         - df: dataframe
    * Return:
         - df: dataframe
    ==========
    Ejemplo:
        >> food_inspection_df = convert_nan(food_inspection_df)
    """

    # creating bool series True for NaN values
    bool_series = pd.isnull(df['facility_type'])
    df.loc[bool_series, 'facility_type'] = "unknown_facility_type"

    bool_series = pd.isnull(df['inspection_type'])
    df.loc[bool_series, 'inspection_type'] = "unknown_inspection_type"

    bool_series = pd.isnull(df['license_'])
    df.loc[bool_series, 'license_'] = "unknown_license"

    bool_series = pd.isnull(df['city'])
    df.loc[bool_series, 'city'] = "unknown_city"


    # para los que tienen ciudad chicago poner estado correcto
    df.loc[df['city'] == 'chicago', 'state'] = 'il'

    bool_series = pd.isnull(df['state'])
    df.loc[bool_series, 'state'] = "unknown_state"


    # creating bool series True for NaN values
    bool_series = pd.isnull(df['risk'])

    # Cambiamos nombres
    # TO-DO : Cambiarlo a regex..extraer lo que esta entre paréntesis
    df.loc[df['risk'] == 'risk 1 (high)', 'risk'] = "high"
    df.loc[df['risk'] == 'risk 2 (medium)', 'risk'] = "medium"
    df.loc[df['risk'] == 'risk 3 (low)', 'risk'] = "low"
    df.loc[bool_series, 'risk'] = 'unknown_risk'

    # zip

    # creating bool series True for NaN values
    bool_series = pd.isnull(df['zip']) & pd.notnull(df['location'])
    zip_notnull_df = df[bool_series]

    geolocator = Nominatim(user_agent='http')

    zipcodes = zip_notnull_df.apply(get_zipcode, axis=1,
                                    geolocator=geolocator,
                                    lat_field='latitude', lon_field='longitude')

    df.loc[bool_series, 'zip'] = zipcodes

    return df


def preprocessing(df):
    """
    Hace todo el preprocesamiento y limpieza de datos:
    > Cambiar a minúsculas las columnas tipo string
    > Cambiar las ciudades que se nombraron mal
    > imputar nulos
    > Transformar la etiqueta

    ==========
    * Args:
         - df: dataframe
    * Return:
         - df: dataframe
    ==========
    Ejemplo:
        >> food_inspection_df = preprocessing(food_inspection_df)
    """
    food_df = df_to_lower_case(df)
    food_df = change_misspelled_chicago_city_names(food_df)
    food_df = convert_nan(food_df)
    food_df = transform_label(food_df)

    return food_df

