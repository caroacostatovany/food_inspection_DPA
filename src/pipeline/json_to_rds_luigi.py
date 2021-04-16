
from datetime import date, timedelta
import luigi
import logging
import pickle
import boto3
import json
import time

from luigi.contrib.s3 import S3Target

from src.pipeline.almacenamiento_luigi import TaskAlmacenamiento
from src.utils.constants import BUCKET_NAME, CREDENCIALES

#["Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

class TaskJson2RDS(luigi.Task):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

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


    def run(self):

        start_time = time.time()
        # Por ahora vamos a borrar el esquema raw y volverlo a crear desde cero e insertar un pkl por pkl..
        # No es lo ideal, pero por simplicidad del ejercicio
        s3 = get_s3_resource()
        objects = s3.list_objects_v2(Bucket=bucket)['Contents']

        # Establecer conexión con la base de datos
        conn = get_db_conn(CREDENCIALES)

        # Leer el sql y ejecutarlo para borrar el esquema y crearlo de nuevo

        table_name = "raw.food_inspection"
        sql_script = "sql/create_raw_food_inspection.sql"
        with conn as cursor:
            cursor.execute(open(sql_script, "r").read())

        num_registros = 0
        # Ahora debemos insertar los json a la tabla vacía
        if len(objects) > 0:

            for file in objects:
                print("Leyendo {}...".format(file))
                json_file = read_pkl_from_s3(s3, bucket, file)

                columns = [list(x.keys()) for x in json_file][0]

                # create a nested list of the records' values
                values = [list(x.values()) for x in json_file]

                # value string for the SQL string
                values_str = ""

                # enumerate over the records' values
                for i, record in enumerate(values):

                    # declare empty list for values
                    val_list = []

                    # append each value to a new list of values
                    for v, val in enumerate(record):
                        if type(val) == str:
                            val = str((val)).replace('"', '')
                        val_list += [str(val)]
                    # print(val_list)

                    # put parenthesis around each record string
                    values_str += "(" + ', '.join(val_list) + "),\n"

                # remove the last comma and end SQL with a semicolon
                values_str = values_str[:-2] + ";"

                sql_string = "INSERT INTO %s (%s)\nVALUES %s" % (
                    table_name,
                    ', '.join(columns),
                    values_str
                )

                cur = conn.cursor()
                cur.execute(sql_string)
                conn.commit()

                # Contamos los registros
                num_registros+=len(values)

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

    def output(self):
        path = "./tmp/luigi/eq3/raw_created.txt"
        return luigi.local_target.LocalTarget(path)


class TaskRawTableMetadata(luigi.Task):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                          "'inicial': Para correr una ingesta inicial. "
                                          "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")
    def requires(self):

        return [TaskJson2RDS(self.ingesta, self.fecha)]

    def run(self):
        path = "./tmp/luigi/eq3/raw_created.txt"
        # Leer el txt y pasarlo a la tabla de metadata
        return  True

    def output(self):
        # Revisar lo de copytotable
        return True