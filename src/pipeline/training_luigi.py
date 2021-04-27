import logging
import pandas as pd
import luigi
import time
from datetime import date, timedelta

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.etl.ingesta_almacenamiento import get_s3_resource
from src.etl.feature_engineering import feature_generation, guardar_feature_engineering, feature_selection
from src.etl.training import fit_training_food
from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3
from src.pipeline.preprocessing_luigi import TaskPreprocessingMetadata
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineeringMetadata
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineering
from src.utils.constants import PATH_LUIGI_TMP, CREDENCIALES, BUCKET_NAME, PATH_TR, NOMBRE_TR, PATH_FE, \
    NOMBRE_FE_xtrain, NOMBRE_FE_ytrain
from src.utils.model_constants import ALGORITHMS

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

    columns = [("date", "varchar"),
               ("algorithm", "varchar"),
               ("best_params", "varchar"),
               ("X_train_file", "varchar"),
               ("y_train_file", "varchar")]

    def requires(self):
        return [TaskTraining(self.ingesta, self.fecha)]

    def rows(self):

        path = "{}/training_created.csv".format(PATH_LUIGI_TMP)
        data = pd.read_csv(path, sep=";")
        r = []
        for i in range(len(data)):
            aux = (data.fecha[i], data.algoritmo[i], data.parametros[i], data.xtrain[i], data.ytrain[i])
            r.append(aux)

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

        # Leyendo datos

        # Leer X_train
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_to_upload_xtrain = '{}/{}'.format(path_s3, NOMBRE_FE_xtrain.format(self.fecha))
        X_train = read_pkl_from_s3(s3, BUCKET_NAME, file_to_upload_xtrain)
        X_train_file = file_to_upload_xtrain.split("/")
        X_train_file = X_train_file[-1]
        X_train_file = X_train_file[:-4]

        # Leer y_train
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_to_upload_ytrain = '{}/{}'.format(path_s3, NOMBRE_FE_ytrain.format(self.fecha))
        y_train = read_pkl_from_s3(s3, BUCKET_NAME, file_to_upload_ytrain)
        y_train_file = file_to_upload_ytrain.split("/")
        y_train_file = y_train_file[-1]
        y_train_file = y_train_file[:-4]


        # Path para guardar
        path = "{}/training_created.csv".format(PATH_LUIGI_TMP)
        file_output = open(path, 'w')
        file_output.write("fecha;algoritmo;parametros;xtrain;ytrain\n")

        # Entrenamiento de modelos
        for algorithm in ALGORITHMS:
            model = fit_training_food(X_train, y_train, algorithm)

            # Guardar best model
            path_s3 = PATH_TR.format(self.fecha.year, self.fecha.month)
            file_to_upload = NOMBRE_TR.format(algorithm, self.fecha)
            path_run = path_s3 + "/" + file_to_upload
            guardar_pkl_en_s3(s3, BUCKET_NAME, path_run, model)
            #guardar_feature_engineering(BUCKET_NAME, path_run, model, CREDENCIALES)

            
            # Debe estar creado el path tmp/luigi/eq3
            file_output.write("{0};{1};{2};{3};{4}\n".format(self.fecha, algorithm,
                                                       model.best_params_,
                                                       X_train_file, y_train_file))
        file_output.close()


    def output(self):
        # Training model
        path_s3 = PATH_TR.format(self.fecha.year, self.fecha.month)
        modelos = []

        for algorithm in ALGORITHMS:
            file_to_upload_tr = NOMBRE_TR.format(algorithm, self.fecha)
            output_path_tr = "s3://{}/{}/{}".format(BUCKET_NAME, path_s3, file_to_upload_tr)
            modelos.append(luigi.contrib.s3.S3Target(path=output_path_tr))

        return modelos
