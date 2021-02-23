"""
Módulo de funciones generales
"""
import yaml
import logging

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
