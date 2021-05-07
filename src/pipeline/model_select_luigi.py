import logging
import pandas as pd
import luigi
from datetime import date

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3, get_s3_resource
from src.utils.constants import S3, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS
from src.pipeline.training_luigi import TaskTrainingMetadata
from src.unit_testing.test_model_select import TestModelSelect
from src.etl.model_select import best_model_selection

logging.basicConfig(level=logging.INFO)


class TaskModelSelectUnitTesting(CopyToTable):
    ingesta = luigi.Parameter(default="No",
                              description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    threshold = luigi.FloatParameter(default=0.80, description="Umbral del desempeño del modelo")

    algoritmo = luigi.Parameter(default="gridsearch",
                                description="gridsearch: Si quieres que cree el modelo dadas las constantes de GridSearch."
                                            "randomclassifier: Si quieres que cree un modelo random classifier."
                                            "decisiontree: Si quieres que cree un modelo con decision tree.")

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "test.unit_testing"
    columns = [("user_id", "varchar"),
               ("modulo", "varchar"),
               ("prueba", "varchar")]

    def requires(self):
        return [TaskModelSelection(self.ingesta, self.fecha, self.threshold, self.algoritmo)]

    def rows(self):
        #s3 = get_s3_resource(CREDENCIALES)

        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        file_to_upload_best_model = NOMBRE_MS.format(self.fecha)

        path_run = path_s3 + "/" + file_to_upload_best_model

        best_model = read_pkl_from_s3(S3, BUCKET_NAME, path_run)

        unit_testing = TestModelSelect()
        unit_testing.test_model_select(best_model)

        r = [(self.user, "model_selection", "test_model_select")]
        for element in r:
            yield element


class TaskModelSelectionMetadata(CopyToTable):

    ingesta = luigi.Parameter(default="No",
                              description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    threshold = luigi.FloatParameter(default=0.80, description="Umbral del desempeño del modelo")

    algoritmo = luigi.Parameter(default="gridsearch",
                                description="gridsearch: Si quieres que cree el modelo dadas las constantes de GridSearch."
                                            "randomclassifier: Si quieres que cree un modelo random classifier."
                                            "decisiontree: Si quieres que cree un modelo con decision tree.")

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "metadata.model_selection"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("dia_ejecucion", "varchar")]

    def requires(self):
        return [TaskModelSelectUnitTesting(self.ingesta, self.fecha, self.threshold, self.algoritmo)]

    def rows(self):
        param = "{0}; {1}".format(self.ingesta, self.fecha)
        r = [(self.user, param, date.today())]
        for element in r:
            yield element


class TaskModelSelection(luigi.Task):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    threshold = luigi.FloatParameter(default=0.80, description="Umbral del desempeño del modelo")

    algoritmo = luigi.Parameter(default="gridsearch",
                                description="gridsearch: Si quieres que cree el modelo dadas las constantes de GridSearch."
                                            "randomclassifier: Si quieres que cree un modelo random classifier."
                                            "decisiontree: Si quieres que cree un modelo con decision tree.")

    best_model = ''

    def requires(self):
        dia = self.fecha
        return [TaskTrainingMetadata(self.ingesta, dia, self.algoritmo)]

    def run(self):

        # Conexión a bucket S3 para extraer datos para modelaje
        #s3 = get_s3_resource(CREDENCIALES)
        objects = S3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']

        # Selección del mejor modelo
        self.best_model = best_model_selection(self.threshold, objects, S3, self.fecha)


        # Guardar best model
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        file_to_upload = NOMBRE_MS.format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_pkl_en_s3(S3, BUCKET_NAME, path_run, self.best_model)
        #guardar_feature_engineering(BUCKET_NAME, path_run, self.best_model, CREDENCIALES)

    def output(self):
        # Best model selection
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        file_to_upload_best_model = NOMBRE_MS.format(self.fecha)
        output_path_best_model = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                        path_s3,
                                                        file_to_upload_best_model)

        return luigi.contrib.s3.S3Target(path=output_path_best_model)
