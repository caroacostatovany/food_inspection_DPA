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

    # Conecatarse por medio del token y usuario generado previamente
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
    results(json): lista de elementos obtenidos de la API
    """

    # Obtener los ultimos "limite" datos
    results = cliente.get("4ijn-s7e5", limit=limite)

    return results

def get_s3_resource(credenciales):
    """
    Crear un resource de S3 para poder guardar los datos en el bucket
    Inputs:
    credenciales: credenciales para poder acceder al bucket
    """

    # Obtener las credenciales del archivo .yaml
    s3_creds = get_s3_credentials(credenciales)

    # Conectarse al bucket
    session = boto3.Session(aws_access_key_id=s3_creds['aws_access_key_id'],
                            aws_secret_access_key=s3_creds['aws_secret_access_key'])

    # Obtener el bucket
    s3 = session.client('s3')

    return s3

def guardar_ingesta(bucket_name, file_to_upload, data, credenciales):
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


def ingesta_consecutiva(cliente, fecha, limite):
    """
    Obtener los datos posteriores a la fecha indicada
    Inputs:
    cleinte(s3): cliente del servicio s3 para conectarse al bucket
    fecha(string): fecha en formato 'Year-month-dayThora', ejemplo: '2021-01-19T00:00:00.000'
    Outputs:
    data_filter(json): datos filtrados por la fecha
    """

    # Obtener los últimos "limite" registros en formato json
    data = ingesta_inicial(cliente, limite)

    # Filtrar por fecha
    data_filter = [x for x in data if x['inspection_date'] >= fecha]
    
    return data_filter
