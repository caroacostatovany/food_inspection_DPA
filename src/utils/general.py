"""
Módulo de funciones generales
"""
import yaml
import logging
import pickle
import psycopg2
import boto3

logging.basicConfig(level=logging.INFO)


def read_yaml_file(yaml_file):
    """
    Carga las Configuración de un .yaml
    ==========
    * Args:
         - yaml_file
    * Return:
         - config: configuraciones del yaml
    ==========
    Ejemplo:
        >> credentials = read_yaml_file(credentials_file)
    """

    config = None
    try:
        with open(yaml_file, 'r') as f:
            config = yaml.safe_load(f)
    except:
        logging.warning("No se pudo leer el archivo yaml")
        raise FileNotFoundError('No se pudo leer el archivo')

    return config


def get_s3_credentials(credentials_file):
    """
    Se obtienen las credenciales para acceder a un s3
    ==========
    * Args:
         - credentials_file
    * Return:
         - s3_creds: Las credenciales existentes en la configuración s3
    ==========
    Ejemplo:
        >> s3_creds = get_s3_credentials("./conf/local/credentials.yaml")
    """
    logging.info("Leyendo las credenciales de {}".format(credentials_file))
    credentials = read_yaml_file(credentials_file)
    s3_creds = credentials['s3']

    return s3_creds


def get_api_token(credentials_file):
    """
    Se obtienen las credenciales para acceder a la API de Chicago Food Inspections
    ==========
    * Args:
         - credentials_file
    * Return:
         - token: Info para conectarse a la API
    Ejemplo:
        >> token = get_api_token("./conf/local/credentials.yaml")
    ==========
    """
    logging.info("Leyendo las credenciales de {}".format(credentials_file))
    credentials = read_yaml_file(credentials_file)
    token = credentials['food_inspections']
    
    return token


def read_pkl_from_s3(s3, bucket_name, filename):
    """
    Lee un archivo que fue guardado con pkl desde s3.

    ==========
    * Args:
         - s3: Session opened
         - bucket_name: nombre del bucket
         - filename: nombre del archivo
    * Return:
         - pkl_file
    ==========
    Ejemplo:
        >> json_file = read_pkl_from_s3(s3, bucket, filename)
    """
    response = s3.get_object(Bucket=bucket_name,
                             Key=filename)
    pkl_file = pickle.loads(response['Body'].read())

    return pkl_file


def get_db_conn_psycopg(credentials_file):
    """
    """
    creds = read_yaml_file(credentials_file)['db']

    connection = psycopg2.connect(
        user=creds['user'],
        password=creds['pass'],
        host=creds['host'],
        port=creds['port'],
        database=creds['db']
    )

    return connection


def get_db(credentials_file):
    """

    """
    creds = read_yaml_file(credentials_file)['db']

    return creds


def guardar_pkl_en_s3(s3, bucket_name, file_to_upload, data):
    """
    Guardar los datos dentro del bucket en el path especificado
    Inputs:
    bucket_name:  bucket s3
    file_to_upload(string): nombre y ruta del archivo a guardar
    data(json): objeto json con los datos
    Outputs:
    None
    """

    # Cambiar datos de formato json a objetos binario
    pickle_dump = pickle.dumps(data)

    # Guardar los datos (pickle) en el bucket y ruta específica
    s3.put_object(Bucket=bucket_name, Key=file_to_upload, Body=pickle_dump)
    logging.info("pkl guardado exitosamente.")


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


def get_db_conn_sql_alchemy(credenciales):
    """Get credentials for db connection"""
    creds = read_yaml_file(credenciales)['db']

    connection = "postgresql://{}:{}@{}:{}/{}".format(creds['user'], creds['pass'], creds['host'], creds['port'],
                                                      creds['db'])

    return connection
