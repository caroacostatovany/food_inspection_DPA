import logging
import pandas as pd
import luigi
import time
from datetime import date, timedelta

from luigi.contrib.postgres import CopyToTable

from src.pipeline.ingesta_almacenamiento import guardar_ingesta, get_s3_resource
from src.pipeline.preprocessing import preprocessing
from src.pipeline.almacenamiento_luigi import TaskAlmacenamientoMetadata
from src.utils.general import get_db, read_pkl_from_s3
from src.utils.constants import CREDENCIALES, BUCKET_NAME, PATH_LUIGI_TMP, PATH_PREPROCESS, NOMBRE_PREPROCESS

logging.basicConfig(level=logging.INFO)


class TaskPreprocessingMetadata(CopyToTable):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    cred = get_db(CREDENCIALES)
    print(cred)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "metadata.preprocessing"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("dia_ejecucion", "varchar"),
               ("tiempo", "float"),
               ("num_registros", "integer")]

    def requires(self):
        return [TaskPreprocessing(self.ingesta, self.fecha)]

    def rows(self):
        path = "{}/preprocess_created.csv".format(PATH_LUIGI_TMP)
        data = pd.read_csv(path)
        r = [(self.user, data.parametros[0], data.dia_ejecucion[0], data.tiempo[0], data.num_registros[0])]
        for element in r:
            yield element


class TaskPreprocessing(luigi.Task):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    def requires(self):

        dia = self.fecha
        if self.ingesta != 'No':
            if self.ingesta == 'inicial':
                return [TaskAlmacenamientoMetadata(True, False, dia)] # Ya lo cambié por el task de metadata de almacenamiento
            else:
                if self.ingesta == 'consecutiva':
                    return [TaskAlmacenamientoMetadata(False, True, dia)] # Ya lo cambié por el task de metadata de almacenamiento
        else:
            while dia.weekday() != 0:
                dia = dia - timedelta(days=1)
            return [TaskAlmacenamientoMetadata (False, True, dia)] # Ya lo cambié por el task de metadata de almacenamiento

    def run(self):
        start_time = time.time()

        s3 = get_s3_resource(CREDENCIALES)
        objects = s3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']

        # Ahora debemos insertar los json a la tabla vacía y sólo leerá los pkl que estan bajo el folder de ingestion
        df = pd.DataFrame()
        if len(objects) > 0:

            for file in objects:
                if file['Key'].find("ingestion/") >= 0 :
                    filename = file['Key']
                    logging.info("Leyendo {}...".format(filename))
                    json_file = read_pkl_from_s3(s3, BUCKET_NAME, filename)
                    df_temp = pd.DataFrame(json_file)
                    df = pd.concat([df, df_temp], axis=0)

        # Contamos los registros
        num_registros = len(df)

        logging.info("Empezemos el preprocesamiento y limpieza de datos...")
        food_df = preprocessing(df)

        end_time = time.time() - start_time

        path = "{}/preprocess_created.csv".format(PATH_LUIGI_TMP)

        file_output = open(path, 'w')
        file_output.write("parametros,dia_ejecucion,tiempo,num_registros\n")
        file_output.write("{0};{1},{2},{3},{4}".format(self.ingesta, self.fecha,
                                                   date.today(),
                                                   end_time,
                                                   num_registros))
        file_output.close()

        path_s3 = PATH_PREPROCESS.format(self.fecha.year, self.fecha.month)
        file_to_upload = NOMBRE_PREPROCESS.format(self.fecha)

        path_run = "{}/{}".format(path_s3, file_to_upload)
        guardar_ingesta(BUCKET_NAME, path_run, food_df, CREDENCIALES)

    def output(self):
        path_s3 = PATH_PREPROCESS.format(self.fecha.year, self.fecha.month)
        file_to_upload = NOMBRE_PREPROCESS.format(self.fecha)

        output_path = "s3://{}/{}/{}".format(BUCKET_NAME,
                                             path_s3,
                                             file_to_upload)
        return luigi.contrib.s3.S3Target(path=output_path)
