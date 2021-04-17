
from datetime import date, timedelta
import luigi
import logging
import pickle
import boto3
import json
import time

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.pipeline.almacenamiento_luigi import TaskAlmacenamiento
from src.utils.constants import BUCKET_NAME, CREDENCIALES
from src.utils.general import get_db, read_pkl_from_s3, get_db_conn_psycopg
from src.pipeline.ingesta_almacenamiento import get_s3_resource

#["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

class TaskJson2RDS(CopyToTable):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    start_time = time.time()
    # Por ahora vamos a borrar el esquema raw y volverlo a crear desde cero e insertar un pkl por pkl..
    # No es lo ideal, pero por simplicidad del ejercicio
    s3 = get_s3_resource(CREDENCIALES)
    objects = s3.list_objects_v2(Bucket=BUCKET_NAME)['Contents']

    # Establecer conexión con la base de datos
    conn = get_db_conn_psycopg(CREDENCIALES)

    # Leer el sql y ejecutarlo para borrar el esquema y crearlo de nuevo

    table_name = "raw.food_inspection"
    sql_script = "sql/create_raw_food_inspection.sql"
    cursor = conn.cursor()
    cursor.execute(open(sql_script, "r").read())
    sites_result = cursor.fetchall()
    cursor.close()
    conn.close()

    num_registros = 0
    # Ahora debemos insertar los json a la tabla vacía
    if len(objects) > 0:
        df = pd.DataFrame()

        for file in objects:
            print("Leyendo {}...".format(file))
            json_file = read_pkl_from_s3(s3, BUCKET_NAME, file)
            df_temp = pd.DataFrame(json_file)
            df = pd.concat([df, df_temp], axis=0)


        # Contamos los registros
        num_registros=len(df)

    end_time = time.time() - start_time


    with self.output().open('w') as output_file:
        output_file.write("{"
                          "{parametros:{0},{1}},"
                          "{dia_ejecucion: {2}},"
                          "{usuario_ejecucion:{3}},"
                          "{tiempo_que_tarda:{4}},"
                          "{num_registros_guardados:{5}},"
                          "{sql_que_ejecuto:{6}},"
                          "{output_sql:{7}}"
                          "{pkl_que_se_utilizaron:{8}}".format(self.ingesta,self.fecha,
                                                            date.today(),
                                                            "-",
                                                            end_time,
                                                            num_registros,
                                                            sql_script,
                                                            output,
                                                            objects))

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table="raw.food_inspection"

    columns = [("inspection_id", "varchar"),
               ("dba_name", "varchar"),
               ("aka_name", "varchar"),
               ("license_", "varchar"),
               ("facility_type", "varchar"),
               ("risk", "varchar"),
               ("address", "varchar"),
               ("city", "varchar"),
               ("state", "varchar"),
               ("inspection_date", "varchar"),
               ("inspection_type", "varchar"),
               ("results", "varchar"),
               ("latitude", "varchar"),
               ("longitude", "varchar"),
               ("location", "json")]


    def requires(self):

        dia = self.fecha
        if self.ingest != 'No':

            if self.ingesta == 'inicial':
                return [TaskAlmacenamiento(True, False, dia)] # Cambiar por el task de metadata de almacenamiento
            else:
                if self.ingesta == 'consecutiva':
                    return [TaskAlmacenamiento(True, False, dia)] # Cambiar por el task de metadata de almacenamiento
        else:
            while dia.weekday != 0:
                dia = dia - timedelta(days=1)
            return [TaskAlmacenamiento(True, False, dia)] # Cambiar por el task de metadata de almacenamiento



    def rows(self):
        tuplas = self.df.to_records(index=False)
        for element in tuplas:
            yield element


class TaskRawTableMetadata(CopyToTable):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
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

    table = "metadata.json2rds"

    columns = [("user_id", "varchar"),
               ("metadata", "json")]

    def requires(self):

        return [TaskJson2RDS(self.ingesta, self.fecha)]


    def rows(self):
        path = "./tmp/luigi/eq3/raw_created.txt"
        f = open(path, "r")
        r = [(self.user, f.read())]
        for element in r:
            yield element
