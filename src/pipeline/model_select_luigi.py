import logging
import pandas as pd
import luigi
import pickle
import time
from datetime import date, timedelta

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.etl.ingesta_almacenamiento import get_s3_resource
from src.etl.feature_engineering import feature_generation, guardar_feature_engineering, feature_selection
from src.etl.training import fit_training_food
from src.utils.general import get_db, read_pkl_from_s3
from src.utils.constants import PATH_LUIGI_TMP, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS, PATH_FE
from src.utils.model_constants import ALGORITHMS
from src.pipeline.training_luigi import TaskTrainingMetadata

logging.basicConfig(level=logging.INFO)


class TaskModelSelectionMetadata(CopyToTable):

    ingesta = luigi.Parameter(default="No",
                              description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    threshold = luigi.FloatParameter(default=0.80, description="Umbral del desempeño del modelo")

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
        return [TaskModelSelection(self.ingesta, self.fecha)]

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

    best_model = ''

    def requires(self):
        dia = self.fecha
        return [TaskTrainingMetadata(self.ingesta, dia)]

    def run(self):

        # Conexión a bucket S3 para extraer datos para modelaje
        s3 = get_s3_resource(CREDENCIALES)
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']

        # Scores
        max_score = 0

        # Leyendo modelos
        if len(objects) > 0:
            for file in objects:
                if file['Key'].find("models/") >= 0:
                    filename = file['Key']
                    logging.info("Leyendo {}...".format(filename))
                    json_file = read_pkl_from_s3(s3, BUCKET_NAME, filename)
                    loaded_model = json_file
                    if loaded_model.best_score_ >= self.threshold:
                        if loaded_model.best_score_ >= max_score:
                            self.best_model = filename
                            best_score = loaded_model.best_score_

        print('\n\n#####Mejor modelo: ', self.best_model)
        print('#####Mejor score: ', best_score)


        # Guardar best model
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        file_to_upload = NOMBRE_MS.format(self.best_model)
        file_to_upload = file_to_upload.split("/")
        file_to_upload = file_to_upload[-1]
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, self.best_model, CREDENCIALES)



    def output(self):
        # Best model selection
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        file_to_upload_best_model = NOMBRE_MS.format(self.best_model)
        file_to_upload_best_model = file_to_upload_best_model.split("/")
        file_to_upload_best_model = file_to_upload_best_model[-1]
        output_path_best_model = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                        path_s3,
                                                        file_to_upload_best_model)

        return luigi.contrib.s3.S3Target(path=output_path_best_model)
