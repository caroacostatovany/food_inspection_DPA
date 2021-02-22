#!/usr/bin/env python
from sodapy import Socrata
import boto3
import pickle
from ..utils.general import *

def get_client():
    """
    Esta función genera un cliente con un token previamente generado
    Inputs:
    None
    Outputs:
    client: cliente que tiene acceso con la cuenta creada previamente
    """

    cliente = Socrata("data.cityofchicago.org",
                      app_token="Ys0XWLepNDhEDms7HrqECMBZe",
                      username="emoreno@itam.mx",
                      password="DPA_food3.")

    return cliente

def ingesta_inicial(cliente,limite=1000):
    """
    Obtener la lista de los elementos que la API generó por medio del cliente
    Inputs:
    cliente: cliente generado con un Token
    limite(int): límite de registros a descargar
    Outputs:
    results(list): lista de elementos obtenidos de la API
    """

    results = cliente.get("4ijn-s7e5", limit=limite)
    return results

def get_s3_resource(credenciales):
    """
    Crear un resource de S3 para poder guardar los datos en el bucket
    Inputs:
    credenciales: credenciales para poder acceder al bucket
    """

    s3_creds = get_s3_credentials(credenciales)
    session = boto3.Session(aws_access_key_id=s3_creds['aws_access_key_id'],
                            aws_secret_access_key=s3_creds['aws_secret_access_key'])
    s3 = session.client('s3')
    return s3

def guardar_ingesta(bucket_name, file_to_upload, data_pkl, credenciales):
    """
    Guardar los datos dentro del bucket en el path especificado
    Inputs:
    bucket_name:  bucket s3
    file_to_upload(string): nombre y ruta del archivo a guardar
    data_pkl: pickle con los datos
    Outputs:
    None
    """
    s3 = get_s3_resource(credenciales)
    s3.put_object(Bucket=bucket_name, Key=file_to_upload, Body=data_pkl)

def ingesta_consecutiva(cliente, fecha, limite):
    """
    Obtener los datos posteriores a la fecha indicada
    Inputs:
    cleinte(s3): cliente del servicio s3 para conectarse al bucket
    fecha(string): fecha en formato 'Year-month-dayThora', ejemplo: '2021-01-19T00:00:00.000'
    Outputs:
    data_filter(json): datos filtrados por la fecha
    """

    data = ingesta_inicial(cliente, limite)

    data_filter = [x for x in data if x['inspection_date'] >= fecha]

    return data_filter
