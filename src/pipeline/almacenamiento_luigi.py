from src.pipeline.ingesta_luigi import TaskIngesta
from datetime import date
import luigi
from luigi.contrib.s3 import S3Target
from src.pipeline.ingesta_almacenamiento import guardar_ingesta, cargar_ingesta_local
from src.utils.constants import BUCKET_NAME, CREDENCIALES, NOMBRE_INICIAL, PATH_INICIAL,\
    NOMBRE_CONSECUTIVO, PATH_CONSECUTIVO


class TaskAlmacenamiento(luigi.Task):

    inicial = luigi.BoolParameter()
    fecha = luigi.DateParameter(default=date.today())




    def requires(self):

        if self.inicial:
            path_s3 = PATH_INICIAL
            file_to_upload = NOMBRE_INICIAL
        else:
            file_to_upload = NOMBRE_CONSECUTIVO
            path_s3 = PATH_CONSECUTIVO

        return TaskIngesta(self.inicial, self.fecha, file_to_upload)

    def run(self):

        if self.inicial:
            path_s3 = PATH_INICIAL
            file_to_upload = NOMBRE_INICIAL
        else:
            file_to_upload = NOMBRE_CONSECUTIVO
            path_s3 = PATH_CONSECUTIVO
        results = cargar_ingesta_local(file_to_upload)
        #outFile = open(self.output().path, 'wb')

        path_run = path_s3+"/"+file_to_upload
        guardar_ingesta(BUCKET_NAME, path_run, results, CREDENCIALES)

        #with self.output().open('w') as output_file:
        #    output_file.write("test,luigi,s3")

    def output(self):

        if self.inicial:
            path_s3 = PATH_INICIAL
            file_to_upload = NOMBRE_INICIAL
        else:
            file_to_upload = NOMBRE_CONSECUTIVO
            path_s3 = PATH_CONSECUTIVO

        output_path = "s3://{}/{}/{}".format(BUCKET_NAME,
                                             path_s3,
                                             file_to_upload)
        return luigi.contrib.s3.S3Target(path=output_path)
