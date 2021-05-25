import logging
import pandas as pd
import luigi
from datetime import date, datetime

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3, get_s3_resource, get_db_conn_psycopg
from src.utils.constants import S3, BUCKET_NAME, CREDENCIALES, PATH_PREPROCESS, NOMBRE_PREPROCESS, NOMBRE_PREDICT, PATH_PREDICT
from src.pipeline.api_luigi import TaskAPI

logging.basicConfig(level=logging.INFO)


class TaskMonitoreo(CopyToTable):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    fecha_modelo = luigi.DateParameter(default=date.today(), description="Fecha del modelo que se quiere utilizar. "
                                                                  "Formato 'año-mes-día'")

    threshold = luigi.FloatParameter(default=0.80, description="Umbral del desempeño del modelo")

    algoritmo = luigi.Parameter(default="gridsearch",
                                description="gridsearch: Si quieres que cree el modelo dadas las constantes de GridSearch."
                                            "randomclassifier: Si quieres que cree un modelo random classifier."
                                            "decisiontree: Si quieres que cree un modelo con decision tree.")

    metrica = luigi.Parameter(default="fpr",
                              description="threshold:"
                                          "precision:"
                                          "recall:"
                                          "f1_score:"
                                          "tpr:"
                                          "fpr:"
                                          "tnr:"
                                          "fnr:")

    kpi = luigi.FloatParameter(default=0.2, description="KPI para la métrica seleccionada")

    strict_probas = luigi.BoolParameter(default=False, description="Revisa que las probabilidades sean estrictamente "
                                                                   "debajo de 1 y arriba de 0")

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "monitoring.scores"

    columns = [('inspection_id', 'integer'),
               ('dba_name', 'varchar'),
               ('label', 'integer'),
               ('predicted_labels', 'integer'),
               ('predicted_score_0', 'float'),
               ('predicted_score_1', 'float'),
               ('model', 'varchar'),
               ('created_at', 'date')]

    def requires(self):
        return [TaskAPI(self.ingesta, self.fecha, self.fecha_modelo, self.threshold, self.algoritmo,
                                    self.metrica, self.kpi, self.strict_probas)]

    def rows(self):
        # Guardar predict
        #path_s3 = PATH_PREPROCESS.format(self.fecha.year, self.fecha.month)
        #filename = "{}/{}".format(path_s3, NOMBRE_PREPROCESS.format(self.fecha))
        #json_file = read_pkl_from_s3(S3, BUCKET_NAME, filename)
        #predict_clean = pd.DataFrame(json_file)
        #predict_clean = predict_clean.set_index('inspection_id')

        #path_s3 = PATH_PREDICT.format(self.fecha.year, self.fecha.month)
        #filename = "{}/{}".format(path_s3, NOMBRE_PREDICT.format(self.fecha))
        #resultados = read_pkl_from_s3(S3, BUCKET_NAME, filename)
        #resultados = resultados.set_index('inspection_id')

        #Unir dataframes por inspection id
        #join_df = pd.concat([predict_clean.loc[resultados.index.astype(str)],
        #                     resultados[['predicted_labels', 'predicted_score_0', 'predicted_score_1', 'model']]], axis=1)
        #join_df = join_df.reset_index()

        conn = get_db_conn_psycopg(CREDENCIALES)

        query = """ select * 
                      from results.scores;
                  """

        join_df = pd.read_sql(query, conn)

        join_df['inspection_id'] = join_df.inspection_id.astype(int)
        join_df['created_at'] = date.today()
        r = join_df.to_records(index=False)

        for element in r:
            yield element
