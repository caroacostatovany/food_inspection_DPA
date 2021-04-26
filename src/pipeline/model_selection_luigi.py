import logging
import pandas as pd
import luigi
import time
from datetime import date, timedelta

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.etl.ingesta_almacenamiento import get_s3_resource
from src.etl.feature_engineering import feature_generation, guardar_feature_engineering, feature_selection
from src.etl.model_selection import magic_loop
from src.utils.general import get_db, read_pkl_from_s3
from src.pipeline.preprocessing_luigi import TaskPreprocessingMetadata
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineeringMetadata
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineering
from src.utils.constants import  PATH_LUIGI_TMP, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS

logging.basicConfig(level=logging.INFO)


class TaskModelSelectionMetadata(CopyToTable):

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

    table = "metadata.feature_engineering"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("dia_ejecucion", "varchar"),
               ("tiempo", "float"),
               ("num_registros", "integer")]

    def requires(self):
        return [TaskModelSelection(self.ingesta, self.fecha)]

    def rows(self):
        path = "{}/feature_engineering_created.csv".format(PATH_LUIGI_TMP)
        data = pd.read_csv(path)
        r = [(self.user, data.parametros[0], data.dia_ejecucion[0], data.tiempo[0], data.num_registros[0])]
        for element in r:
            yield element


class TaskModelSelection(luigi.Task):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    def requires(self):
        dia = self.fecha
        return [TaskModelSelectionMetadata(self.ingesta, dia)]

    def run(self):

        # Conexión a bucket S3 para extraer datos para modelaje
        s3 = get_s3_resource(CREDENCIALES)
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']

        # Leyendo datos
        list_data = []
        if len(objects) > 0:
            for file in objects:
                if file['Key'].find("feature_engineering/") >= 0:
                    filename = file['Key']
                    logging.info("Leyendo {}...".format(filename))
                    json_file = read_pkl_from_s3(s3, BUCKET_NAME, filename)
                    df_temp = pd.DataFrame(json_file)
                    list_data.append(df_temp)

        # Feature generation
        logging.info("Realizando model selection")
        X_train = list_data[1]
        y_train = list_data[2]

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
