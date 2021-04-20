import logging
import pandas as pd
import luigi
import time
from datetime import date, timedelta

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.pipeline.ingesta_almacenamiento import get_s3_resource
from src.pipeline.feature_engineering import feature_generation, guardar_feature_engineering, feature_selection
from src.utils.general import get_db, read_pkl_from_s3
from src.utils.constants import CREDENCIALES, BUCKET_NAME
from src.pipeline.preprocessing_luigi import TaskPreprocessingMetadata

logging.basicConfig(level=logging.INFO)


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
        path = "./tmp/luigi/eq3/feature_engineering_created.csv"
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

        # Fin de tiempo para feature engineering
        end_time = time.time() - start_time

        # Path para guardar
        path = "./tmp/luigi/eq3/feature_engineering_created.csv"

        # Debe estar creado el path tmp/luigi/eq3
        file_output = open(path,'w')
        file_output.write("parametros,dia_ejecucion,tiempo,num_registros\n")
        file_output.write("{0};{1},{2},{3},{4}".format(self.ingesta, self.fecha,
                                                   date.today(),
                                                   end_time,
                                                   num_registros))
        file_output.close()

        # Guardar food_df
        path_s3 = "feature_engineering_full/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload = "feature_engineering_data_full_{}.pkl".format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, food_df, CREDENCIALES)

        # Guardar X_train
        path_s3 = "feature_engineering_X_train/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload = "feature_engineering_data_X_train_{}.pkl".format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, X_train, CREDENCIALES)

        # Guardar X_test
        path_s3 = "feature_engineering_X_test/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload = "feature_engineering_data_X_test_{}.pkl".format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, X_test, CREDENCIALES)

        # Guardar y_train
        path_s3 = "feature_engineering_y_train/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload = "feature_engineering_data_y_train_{}.pkl".format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, X_train, CREDENCIALES)

        # Guardar y_test
        path_s3 = "feature_engineering_y_test/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload = "feature_engineering_data_y_test_{}.pkl".format(self.fecha)
        path_run = path_s3 + "/" + file_to_upload
        guardar_feature_engineering(BUCKET_NAME, path_run, X_test, CREDENCIALES)

    def output(self):
        # Full
        path_s3_full = "feature_engineering_full/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload_full = "feature_engineering_data_full_{}.pkl".format(self.fecha)
        output_path_full = "s3://{}/{}/{}".format(BUCKET_NAME,
                                             path_s3_full,
                                             file_to_upload_full)

        # X_train
        path_s3_xtrain = "feature_engineering_X_train/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload_xtrain = "feature_engineering_data_X_train_{}.pkl".format(self.fecha)
        output_path_xtrain = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                  path_s3_xtrain,
                                                  file_to_upload_xtrain)

        # X_test
        path_s3_xtest = "feature_engineering_X_test/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload_xtest = "feature_engineering_data_X_test_{}.pkl".format(self.fecha)
        output_path_xtest = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                    path_s3_xtest,
                                                    file_to_upload_xtest)

        # y_train
        path_s3_ytrain = "feature_engineering_y_train/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload_ytrain = "feature_engineering_data_y_train_{}.pkl".format(self.fecha)
        output_path_ytrain = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                    path_s3_ytrain,
                                                    file_to_upload_ytrain)

        # y_test
        path_s3_ytest = "feature_engineering_y_test/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload_ytest = "feature_engineering_data_y_test_{}.pkl".format(self.fecha)
        output_path_ytest = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                   path_s3_ytest,
                                                   file_to_upload_ytest)

        return luigi.contrib.s3.S3Target(path=output_path_full), \
               luigi.contrib.s3.S3Target(path=output_path_xtrain), \
               luigi.contrib.s3.S3Target(path=output_path_xtest), \
               luigi.contrib.s3.S3Target(path=output_path_ytrain), \
               luigi.contrib.s3.S3Target(path=output_path_ytest)
