from src.pipeline.ingesta_luigi import TaskIngesta
from datetime import date
import luigi
from luigi.contrib.s3 import S3Target
from src.pipeline.ingesta_almacenamiento import guardar_ingesta, cargar_ingesta_local
from src.utils.constants import BUCKET_NAME, CREDENCIALES, NOMBRE_INICIAL, PATH_INICIAL,\
    NOMBRE_CONSECUTIVO, PATH_CONSECUTIVO


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

        return [TaskIngesta(self.ingesta_inicial, self.fecha, file_to_upload)] #Cambiar a TaskIngestaMetadata

    def run(self):

        if self.ingesta_inicial:
            path_s3 = PATH_INICIAL.format(self.fecha.year, self.fecha.month)
            file_to_upload = NOMBRE_INICIAL.format(self.fecha)
        else:
            file_to_upload = NOMBRE_CONSECUTIVO.format(self.fecha)
            path_s3 = PATH_CONSECUTIVO.format(self.fecha.year, self.fecha.month)
        results = cargar_ingesta_local(file_to_upload)
        #outFile = open(self.output().path, 'wb')

        path_run = path_s3+"/"+file_to_upload
        guardar_ingesta(BUCKET_NAME, path_run, results, CREDENCIALES)

        #with self.output().open('w') as output_file:
        #    output_file.write("test,luigi,s3")

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
