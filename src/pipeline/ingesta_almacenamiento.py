"""
Módulo de funciones para ingesta y almacenamiento
"""
import boto3
import pickle

from datetime import date, timedelta
from sodapy import Socrata
from src.utils.general import get_s3_credentials, get_api_token, logging
from src.utils.constants import CREDENCIALES, BUCKET_NAME, PATH_LUIGI_TMP


def get_client():
    """
    Esta función genera un cliente con un token previamente generado
    Inputs:
    credenciales: credenciales para poder acceder al bucket
    Outputs:
    client: cliente que tiene acceso con la cuenta creada previamente
    """

    # Obtener las credenciales del archivo .yaml
    token = get_api_token(CREDENCIALES)
    
    # Conectarse a la API
    logging.info("Obteniendo cliente desde Socrata..")
    
    # Conectarse por medio del token y usuario generado previamente
    cliente = Socrata("data.cityofchicago.org",
                      app_token=token['app_token'],
                      username=token['username'],
                      password=token['password'])
    
    return cliente


def ingesta_inicial(cliente, limite=300000):
    """
    Obtener la lista de los elementos que la API generó por medio del cliente
    Inputs:
    cliente: cliente generado con un Token
    limite(int): límite de registros a descargar
    Outputs:
    results(json): lista de elementos obtenidos de la API
    """

    # Obtener los ultimos "limite" datos
    logging.info("Obteniendo todos los resultados de Chicago food inspections... ")
    results = cliente.get("4ijn-s7e5", limit=limite)
    logging.info("Listo!")


    # logging.info("Guardando la ingesta inicial en s3://{}/{}".format(BUCKET_NAME, file_to_upload))
    # guardar_ingesta(BUCKET_NAME, file_to_upload, results, CREDENCIALES)

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
    logging.info("Abriendo sesión s3")
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
    logging.info("pkl guardado exitosamente.")


def guardar_ingesta_localmente(file_to_upload, data):
    """
    Guardar los datos dentro del path file_to_upload
    Inputs:
    file_to_upload(string): nombre y ruta del archivo a guardar
    data(json): objeto json con los datos
    Outputs:
    None
    """
    # Cambiar datos de formato json a objetos binario
    pickle.dump(data, file_to_upload)
    logging.info("pkl guardado exitosamente en local.")
    file_to_upload.close()


def cargar_ingesta_local(file_to_upload):
    """
    Guardar los datos dentro del path tmp/luigi
    Inputs:
    file_to_upload(string): nombre y ruta del archivo a guardar
    data(json): objeto json con los datos
    Outputs:
    None
    """
    path = file_to_upload
    # Cambiar datos de formato json a objetos binario
    pkl = pickle.load(open(path, "rb"))
    logging.info("pkl cargado exitosamente.")
    return pkl


def ingesta_consecutiva(cliente, fecha, limite=1000):
    """
    Obtener los datos posteriores a la fecha indicada
    Inputs:
    cleinte(s3): cliente del servicio s3 para conectarse al bucket
    fecha(string): fecha en formato 'Year-month-dayThora', ejemplo: '2021-01-19T00:00:00.000'
    Outputs:
    data_filter(json): datos filtrados por la fecha
    """

    logging.info("Obteniendo todos los resultados de Chicago food insepctions a partir de la fecha: {}".format(fecha))
    # Obtener los últimos "limite" registros en formato json

    fecha_inicio = fecha - timedelta(days=7)
    # Obtener data entre fechas con límite
    data = cliente.get("4ijn-s7e5",
                       limit=limite,
                       where="inspection_date between '{}' and '{}'".format(fecha_inicio, str(fecha)))


    #logging.info("Guardando la ingesta consecutiva en s3://{}/{}".format(BUCKET_NAME, file_to_upload))
    #guardar_ingesta(BUCKET_NAME, file_to_upload, data, CREDENCIALES)

    return data
