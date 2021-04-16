"""
Módulo para constantes
"""
from datetime import date

CREDENCIALES = "conf/local/credentials.yaml"
BUCKET_NAME = "data-product-architecture-equipo-3"

# Puedes cambiarlos también si así no deseas que se guarden
PATH_INICIAL = "ingestion/initial/YEAR={}/MONTH={}"
NOMBRE_INICIAL = "historic-inspections-{}.pkl"

PATH_CONSECUTIVO = "ingestion/consecutive/YEAR={}/MONTH={}"
NOMBRE_CONSECUTIVO = "consecutive-inspections-{}.pkl"


# Definimos la lista de lo que queremos para nuestro feature engineering
L = ['year', 'month', 'day', 'dayofweek', 'dayofyear', 'week', 'quarter']