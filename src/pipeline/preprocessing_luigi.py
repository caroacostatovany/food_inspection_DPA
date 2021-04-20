import logging
import pandas as pd
import luigi
import time
from datetime import date, timedelta

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.pipeline.ingesta_almacenamiento import guardar_ingesta, get_s3_resource
from src.pipeline.preprocessing import df_to_lower_case, change_misspelled_chicago_city_names, convert_nan, transform_label
from src.utils.general import get_db, read_pkl_from_s3
from src.utils.constants import CREDENCIALES, BUCKET_NAME
from src.pipeline.almacenamiento_luigi import TaskAlmacenamiento

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
        path = "./tmp/luigi/eq3/preprocess_created.csv"
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
                return [TaskAlmacenamiento(True, False, dia)] # Cambiar por el task de metadata de almacenamiento
            else:
                if self.ingesta == 'consecutiva':
                    return [TaskAlmacenamiento(False, True, dia)] # Cambiar por el task de metadata de almacenamiento
        else:
            while dia.weekday() != 0:
                dia = dia - timedelta(days=1)
            return [TaskAlmacenamiento(False, True, dia)] # Cambiar por el task de metadata de almacenamiento

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
                if file['Key'].find("ingestion/") >= 0 :
                    filename = file['Key']
                    logging.info("Leyendo {}...".format(filename))
                    json_file = read_pkl_from_s3(s3, BUCKET_NAME, filename)
                    df_temp = pd.DataFrame(json_file)
                    df = pd.concat([df, df_temp], axis=0)

        # Contamos los registros
        num_registros = len(df)

        food_df = df_to_lower_case(df)
        # Creo que esta parte se puede hacer mejor en sql
        food_df = change_misspelled_chicago_city_names(food_df)
        # Creo que esta parte se puede hacer mejor en sql
        food_df = convert_nan(food_df)
        food_df = transform_label(food_df)

        end_time = time.time() - start_time

        path = "./tmp/luigi/eq3/preprocess_created.csv"

        # Debe estar creado el path tmp/luigi/eq3
        file_output = open(path,'w')
        file_output.write("parametros,dia_ejecucion,tiempo,num_registros\n")
        file_output.write("{0};{1},{2},{3},{4}".format(self.ingesta, self.fecha,
                                                   date.today(),
                                                   end_time,
                                                   num_registros))
        file_output.close()

        path_s3 = "preprocessing/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload = "clean_data_{}.pkl".format(self.fecha)

        path_run = path_s3 + "/" + file_to_upload
        guardar_ingesta(BUCKET_NAME, path_run, food_df, CREDENCIALES)

    def output(self):
        path_s3 = "preprocessing/{}/{}".format(self.fecha.year, self.fecha.month)
        file_to_upload = "clean_data_{}.pkl".format(self.fecha)
        output_path = "s3://{}/{}/{}".format(BUCKET_NAME,
                                             path_s3,
                                             file_to_upload)
        return luigi.contrib.s3.S3Target(path=output_path)
