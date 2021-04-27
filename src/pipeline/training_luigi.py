import logging
import pandas as pd
import luigi
import time
from datetime import date, timedelta

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.etl.ingesta_almacenamiento import get_s3_resource
from src.etl.feature_engineering import feature_generation, guardar_feature_engineering, feature_selection
from src.etl.training import magic_loop
from src.utils.general import get_db, read_pkl_from_s3
from src.pipeline.preprocessing_luigi import TaskPreprocessingMetadata
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineeringMetadata
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineering
from src.utils.constants import PATH_LUIGI_TMP, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS, PATH_FE, \
    NOMBRE_FE_xtrain, NOMBRE_FE_ytrain

logging.basicConfig(level=logging.INFO)


class TaskTrainingMetadata(CopyToTable):

    ingesta = luigi.Parameter(default="No",
                              description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "metadata.training"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("dia_ejecucion", "varchar")]

    def requires(self):
        return [TaskTraining(self.ingesta, self.fecha)]

    def rows(self):
        param = "{0}; {1}".format(self.ingesta, self.fecha)
        r = [(self.user, param, date.today())]
        for element in r:
            yield element


class TaskTraining(luigi.Task):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    def requires(self):
        dia = self.fecha
        return [TaskFeatureEngineeringMetadata(self.ingesta, dia)]

    def run(self):

        # Conexión a bucket S3 para extraer datos para modelaje
        s3 = get_s3_resource(CREDENCIALES)
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']

        # Leyendo datos

        # Leer X_train
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_to_upload_xtrain = '{}/{}'.format(path_s3, NOMBRE_FE_xtrain.format(self.fecha))
        X_train = read_pkl_from_s3(s3, BUCKET_NAME, file_to_upload_xtrain)

        # Leer y_train
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_to_upload_ytrain = '{}/{}'.format(path_s3, NOMBRE_FE_ytrain.format(self.fecha))
        y_train = read_pkl_from_s3(s3, BUCKET_NAME, file_to_upload_ytrain)
        print(y_train)

        # Entrenamiento de modelo
        best_model = magic_loop(X_train, y_train)

        # Guardar best model
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        file_to_upload = NOMBRE_MS.format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, best_model, CREDENCIALES)



    def output(self):
        # Best model selection
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        file_to_upload_ms = NOMBRE_MS.format(self.fecha)
        output_path_ms = "s3://{}/{}/{}".format(BUCKET_NAME, path_s3, file_to_upload_ms)

        return luigi.contrib.s3.S3Target(path=output_path_ms)
