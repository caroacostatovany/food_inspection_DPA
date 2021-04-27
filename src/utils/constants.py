"""
Módulo para constantes
"""

# PATH de las credenciales
CREDENCIALES = "conf/local/credentials.yaml"

# Nombre del bucket name
BUCKET_NAME = "data-product-architecture-equipo-3"

# Luigi tmp path
PATH_LUIGI_TMP = "./tmp/luigi/eq3"

# Puedes cambiarlos también si así no deseas que se guarden
PATH_INICIAL = "ingestion/initial/YEAR={}/MONTH={}"
NOMBRE_INICIAL = "historic-inspections-{}.pkl"

PATH_CONSECUTIVO = "ingestion/consecutive/YEAR={}/MONTH={}"
NOMBRE_CONSECUTIVO = "consecutive-inspections-{}.pkl"


# Preprocessing:
PATH_PREPROCESS = "preprocessing/YEAR={}/MONTH={}"
NOMBRE_PREPROCESS = "clean_data_{}.pkl"

# Feature engineering:
PATH_FE = "feature_engineering/YEAR={}/MONTH={}"
NOMBRE_FE_full = "feature_engineering_inspections_full_{}.pkl"
NOMBRE_FE_xtrain = "feature_engineering_inspections_x_train_{}.pkl"
NOMBRE_FE_xtest = "feature_engineering_inspections_x_test_{}.pkl"
NOMBRE_FE_ytrain = "feature_engineering_inspections_y_train_{}.pkl"
NOMBRE_FE_ytest = "feature_engineering_inspections_y_test_{}.pkl"

# Training:
PATH_TR = "models/YEAR={}/MONTH={}"
NOMBRE_TR = "models_{}_{}.pkl"

# Definimos la lista de lo que queremos para nuestro feature engineering
L = ['year', 'month', 'day', 'dayofweek', 'dayofyear', 'week', 'quarter']