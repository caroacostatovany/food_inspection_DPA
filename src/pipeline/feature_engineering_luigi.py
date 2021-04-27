import logging
import pandas as pd
import luigi
import time
from datetime import date, timedelta

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.etl.ingesta_almacenamiento import get_s3_resource
from src.etl.feature_engineering import feature_generation, guardar_feature_engineering, feature_selection
from src.utils.general import get_db, read_pkl_from_s3
from src.pipeline.preprocessing_luigi import TaskPreprocessingMetadata
from src.utils.constants import PATH_FE, NOMBRE_FE_xtest, NOMBRE_FE_xtrain, NOMBRE_FE_ytest, NOMBRE_FE_ytrain, \
    NOMBRE_FE_full, PATH_LUIGI_TMP, CREDENCIALES, BUCKET_NAME
from src.unit_testing.test_feature_engineering import TestFeatureEngineering

logging.basicConfig(level=logging.INFO)

class TaskFeatureEngineeringUnitTesting(CopyToTable):

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

    table = "test.unit_testing"

    columns = [("user_id", "varchar"),
               ("modulo", "varchar"),
               ("prueba", "varchar")]

    def requires(self):
        return [TaskFeatureEngineering(self.ingesta, self.fecha)]

    def rows(self):

        unit_testing = TestFeatureEngineering()
        #path = "{}/{}".format(PATH_LUIGI_TMP, self.file_to_upload)
        #unit_testing.test_ingesta(path)
        #r = [(self.user, "ingesta", "test_ingesta")]
        #for element in r:
        #    yield element


class TaskFeatureEngineeringMetadata(CopyToTable):

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
        return [TaskFeatureEngineering(self.ingesta, self.fecha)]

    def rows(self):
        path = "{}/feature_engineering_created.csv".format(PATH_LUIGI_TMP)
        data = pd.read_csv(path)
        r = [(self.user, data.parametros[0], data.dia_ejecucion[0], data.tiempo[0], data.num_registros[0])]
        for element in r:
            yield element


class TaskFeatureEngineering(luigi.Task):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    def requires(self):
        dia = self.fecha
        return [TaskPreprocessingMetadata(self.ingesta, dia)]

    def run(self):
        start_time = time.time()

        # Por ahora vamos a borrar el esquema raw y volverlo a crear desde cero e insertar un pkl por pkl..
        # No es lo ideal, pero por simplicidad del ejercicio
        s3 = get_s3_resource(CREDENCIALES)
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']

        # Leer el sql y ejecutarlo para borrar el esquema y crearlo de nuevo

        # Ahora debemos insertar los json a la tabla vacía
        df = pd.DataFrame()

        if len(objects) > 0:
            for file in objects:
                if file['Key'].find("preprocessing/") >= 0:
                    filename = file['Key']
                    logging.info("Leyendo {}...".format(filename))
                    json_file = read_pkl_from_s3(s3, BUCKET_NAME, filename)
                    df_temp = pd.DataFrame(json_file)
                    df = pd.concat([df, df_temp], axis=0)


        # Contamos los registros
        num_registros = len(df)

        # Feature generation
        logging.info("Realizando feature generation")
        food_df = feature_generation(df)

        # Feature selection
        logging.info("Realizando feature selection")
        X_train, X_test, y_train, y_test = feature_selection(food_df)
        print(y_train)

        # Fin de tiempo para feature engineering
        end_time = time.time() - start_time

        # Path para guardar
        path = "{}/feature_engineering_created.csv".format(PATH_LUIGI_TMP)

        # Debe estar creado el path tmp/luigi/eq3
        file_output = open(path,'w')
        file_output.write("parametros,dia_ejecucion,tiempo,num_registros\n")
        file_output.write("{0};{1},{2},{3},{4}".format(self.ingesta, self.fecha,
                                                   date.today(),
                                                   end_time,
                                                   num_registros))
        file_output.close()

        # Guardar food_df
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_to_upload = NOMBRE_FE_full.format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, food_df, CREDENCIALES)

        # Guardar X_train
        file_to_upload = NOMBRE_FE_xtrain.format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, X_train, CREDENCIALES)

        # Guardar X_test
        file_to_upload = NOMBRE_FE_xtest.format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, X_test, CREDENCIALES)

        # Guardar y_train
        file_to_upload = NOMBRE_FE_ytrain.format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, y_train, CREDENCIALES)

        # Guardar y_test
        file_to_upload = NOMBRE_FE_ytest.format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, y_test, CREDENCIALES)

    def output(self):
        # Full
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_to_upload_full = NOMBRE_FE_full.format(self.fecha)
        output_path_full = "s3://{}/{}/{}".format(BUCKET_NAME,
                                             path_s3,
                                             file_to_upload_full)

        # X_train
        file_to_upload_xtrain = NOMBRE_FE_xtrain.format(self.fecha)
        output_path_xtrain = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                  path_s3,
                                                  file_to_upload_xtrain)

        # X_test
        file_to_upload_xtest = NOMBRE_FE_xtest.format(self.fecha)
        output_path_xtest = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                    path_s3,
                                                    file_to_upload_xtest)

        # y_train
        file_to_upload_ytrain = NOMBRE_FE_ytrain.format(self.fecha)
        output_path_ytrain = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                    path_s3,
                                                    file_to_upload_ytrain)

        # y_test
        file_to_upload_ytest = NOMBRE_FE_ytest.format(self.fecha)
        output_path_ytest = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                   path_s3,
                                                   file_to_upload_ytest)

        return luigi.contrib.s3.S3Target(path=output_path_full), \
               luigi.contrib.s3.S3Target(path=output_path_xtrain), \
               luigi.contrib.s3.S3Target(path=output_path_xtest), \
               luigi.contrib.s3.S3Target(path=output_path_ytrain), \
               luigi.contrib.s3.S3Target(path=output_path_ytest)
