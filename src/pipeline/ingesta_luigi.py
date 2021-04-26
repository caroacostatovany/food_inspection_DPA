
from datetime import date
import luigi
import logging

from luigi.contrib.postgres import CopyToTable

from src.etl.ingesta_almacenamiento import get_client, ingesta_inicial, ingesta_consecutiva, \
    guardar_ingesta_localmente
from src.utils.constants import PATH_LUIGI_TMP, CREDENCIALES
from src.utils.general import get_db
from src.unit_testing.test_ingesta import TestIngesta


logging.basicConfig(level=logging.INFO)


class TaskIngestaUnitTesting(CopyToTable):
    inicial = luigi.BoolParameter()
    fecha = luigi.DateParameter()
    file_to_upload = luigi.Parameter()

    cred = get_db(CREDENCIALES)
    print(cred)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "test.unit_testing"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("task", "varchar")]

    unit_testing = TestIngesta()
    print(unit_testing)

    def requires(self):
        return [TaskIngesta(self.inicial, self.fecha, self.file_to_upload)]

    def rows(self):
        param = "{0}; {1}; {2}".format(self.inicial, self.fecha, self.file_to_upload)
        r = [(self.user, param, "ingesta")]
        for element in r:
            yield element


class TaskIngestaMetadata(CopyToTable):

    inicial = luigi.BoolParameter()
    fecha = luigi.DateParameter()
    file_to_upload = luigi.Parameter()

    cred = get_db(CREDENCIALES)
    print(cred)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "metadata.ingesta"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("dia_ejecucion", "varchar")]

    def requires(self):
        return [TaskIngestaUnitTesting(self.inicial, self.fecha, self.file_to_upload)]

    def rows(self):
        param = "{0}; {1}; {2}".format(self.inicial, self.fecha, self.file_to_upload)
        r = [(self.user, param, date.today())]
        for element in r:
            yield element


class TaskIngesta(luigi.Task):

    inicial = luigi.BoolParameter()
    fecha = luigi.DateParameter()
    file_to_upload = luigi.Parameter()

    def run(self):
        cliente = get_client()

        if self.inicial:
            results = ingesta_inicial(cliente)
        else:
            results = ingesta_consecutiva(cliente, self.fecha, limite=1000)

        outfile = open(self.output().path, 'wb')
        guardar_ingesta_localmente(outfile, data=results)

    def output(self):
        path = "{}/{}".format(PATH_LUIGI_TMP, self.file_to_upload)
        return luigi.local_target.LocalTarget(path)
