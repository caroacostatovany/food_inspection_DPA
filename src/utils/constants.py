"""
Módulo para constantes
"""
from datetime import date
CREDENCIALES = "conf/local/credentials.yaml"
BUCKET_NAME = "data-product-architecture-equipo-3"

PATH_INICIAL = "ingestion/initial"
NOMBRE_INICIAL = "historic-inspections-{}.pkl".format(date.today())

PATH_CONSECUTIVO = "ingestion/consecutive"
NOMBRE_CONSECUTIVO = "consecutive-inspections-{}.pkl".format(date.today())