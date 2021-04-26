from datetime import date
import luigi

from luigi.contrib.postgres import CopyToTable
from luigi.contrib.s3 import S3Target
from src.utils.constants import BUCKET_NAME, CREDENCIALES, NOMBRE_INICIAL, PATH_INICIAL,\
    NOMBRE_CONSECUTIVO, PATH_CONSECUTIVO, PATH_LUIGI_TMP
from src.utils.general import get_db
from src.etl.ingesta_almacenamiento import guardar_ingesta, cargar_ingesta_local
from src.pipeline.ingesta_luigi import TaskIngestaMetadata
from src.unit_testing.test_almacenamiento import TestAlmacenamiento


class TaskAlmacenamientoUnitTesting(CopyToTable):

    ingesta_inicial = luigi.BoolParameter(description="Parámetro booleano. Si se escribe será Verdadero, "
                                                      "para crear la ingesta inicial")
    ingesta_consecutiva = luigi.BoolParameter(description="Parámetro booleano. Si se escribe será Verdadero, "
                                                          "para crear la ingesta consecutiva")
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
        return [TaskAlmacenamiento(self.ingesta_inicial, self.ingesta_consecutiva, self.fecha)]

    def rows(self):
        if self.ingesta_inicial:
            path_s3 = PATH_INICIAL.format(self.fecha.year, self.fecha.month)
            file_to_upload = NOMBRE_INICIAL.format(self.fecha)
        else:
            file_to_upload = NOMBRE_CONSECUTIVO.format(self.fecha)
            path_s3 = PATH_CONSECUTIVO.format(self.fecha.year, self.fecha.month)

        path = "{}/{}".format(path_s3,file_to_upload)
        unit_testing = TestAlmacenamiento()
        unit_testing.test_almacenamiento_json(path)

        r = [(self.user, "almacenamiento", "test_almacenamiento_json")]
        for element in r:
            yield element


class TaskAlmacenamientoMetadata(CopyToTable):

    ingesta_inicial = luigi.BoolParameter(description="Parámetro booleano. Si se escribe será Verdadero, "
                                                      "para crear la ingesta inicial")
    ingesta_consecutiva = luigi.BoolParameter(description="Parámetro booleano. Si se escribe será Verdadero, "
                                                          "para crear la ingesta consecutiva")
    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "metadata.almacenamiento"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("dia_ejecucion", "varchar")]

    def requires(self):
        return [TaskAlmacenamientoUnitTesting(self.ingesta_inicial, self.ingesta_consecutiva, self.fecha)]

    def rows(self):
        param = "{0}; {1}; {2}".format(self.ingesta_inicial, self.ingesta_consecutiva, self.fecha)
        r = [(self.user, param, date.today())]
        for element in r:
            yield element


class TaskAlmacenamiento(luigi.Task):

    ingesta_inicial = luigi.BoolParameter(description="Parámetro booleano. Si se escribe será Verdadero, "
                                                      "para crear la ingesta inicial")
    ingesta_consecutiva = luigi.BoolParameter(description="Parámetro booleano. Si se escribe será Verdadero, "
                                                          "para crear la ingesta consecutiva")
    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    def requires(self):

        if self.ingesta_inicial:
            file_to_upload = NOMBRE_INICIAL.format(self.fecha)
        else:
            file_to_upload = NOMBRE_CONSECUTIVO.format(self.fecha)

        return [TaskIngestaMetadata(self.ingesta_inicial, self.fecha, file_to_upload)]

    def run(self):

        if self.ingesta_inicial:
            path_s3 = PATH_INICIAL.format(self.fecha.year, self.fecha.month)
            file_to_upload = NOMBRE_INICIAL.format(self.fecha)
        else:
            file_to_upload = NOMBRE_CONSECUTIVO.format(self.fecha)
            path_s3 = PATH_CONSECUTIVO.format(self.fecha.year, self.fecha.month)

        path = "{}/{}".format(PATH_LUIGI_TMP, file_to_upload)
        results = cargar_ingesta_local(path)

        path_run = "{}/{}".format(path_s3, file_to_upload)
        guardar_ingesta(BUCKET_NAME, path_run, results, CREDENCIALES)

    def output(self):
        if self.ingesta_inicial:
            path_s3 = PATH_INICIAL.format(self.fecha.year, self.fecha.month)
            file_to_upload = NOMBRE_INICIAL.format(self.fecha)

        else:
            path_s3 = PATH_CONSECUTIVO.format(self.fecha.year, self.fecha.month)
            file_to_upload = NOMBRE_CONSECUTIVO.format(self.fecha)

        output_path = "s3://{}/{}/{}".format(BUCKET_NAME,
                                             path_s3,
                                             file_to_upload)
        return luigi.contrib.s3.S3Target(path=output_path)
