
from datetime import date
import luigi
import logging
import pickle
import boto3
from luigi.contrib.s3 import S3Target
from src.pipeline.ingesta_almacenamiento import get_client, ingesta_inicial, ingesta_consecutiva, \
    guardar_ingesta_localmente


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

        outFile = open(self.output().path, 'wb')
        guardar_ingesta_localmente(outFile, data=results)

    def output(self):
        path = "./tmp/luigi/{}".format(self.file_to_upload)
        return luigi.local_target.LocalTarget(path)
        # return luigi.contrib.s3.S3Target('s3://{}/{}'.format(BUCKET_NAME, self.file_to_upload))