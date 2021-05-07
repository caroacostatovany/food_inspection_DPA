import logging
import pandas as pd
import luigi
from datetime import date

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3, get_s3_resource
from src.utils.constants import S3, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS, PATH_FE, NOMBRE_FE_xtest, NOMBRE_FE_ytest, REF_GROUPS_DICT
from src.pipeline.model_select_luigi import TaskModelSelectionMetadata
from src.etl.metricas import get_metrics_matrix

logging.basicConfig(level=logging.INFO)


class TaskMetricas(CopyToTable):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
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


    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "semantic.aequitas"

    ## Esto puede cambiar, no? depende de cuál sean las que estan dadas de alta en REF_GROUPS_DICT

    columns = []
    for key in list(REF_GROUPS_DICT.keys()):
        tupla = (key, "varchar")
        columns.append(tupla)

    columns.append(("label_value", "integer"))
    columns.append(("score", "integer"))

    def requires(self):
        return [TaskModelSelectionMetadata(self.ingesta, self.fecha, self.threshold, self.algoritmo)]

    def rows(self):

        # Leemos x_test y y_test
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_xtest = NOMBRE_FE_xtest.format(self.fecha)
        filename = "{}/{}".format(path_s3, file_xtest)
        X_test = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        file_ytest = NOMBRE_FE_xtest.format(self.fecha)
        filename = "{}/{}".format(path_s3, file_ytest)
        y_test = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        # Leer el mejor modelo
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        filename = path_s3 + "/" + NOMBRE_MS.format(self.fecha)
        print("############################################################################",filename)
        best_model = read_pkl_from_s3(S3, BUCKET_NAME, filename)
        print("#####################################################################",best_model)
        model = read_pkl_from_s3(S3, BUCKET_NAME, best_model)

        predicted_scores = model.predict_proba(X_test)

        path_s3 = PATH_PREPROCESS.format(self.fecha.year, self.fecha.month)
        preprocess_file = "{}/{}".format(path_s3, NOMBRE_PREPROCESS.format(self.fecha))
        clean_data = read_pkl_from_s3(S3, BUCKET_NAME, preprocess_file)

        metricas = get_metrics_matrix(y_test, predicted_scores)

        metrica = self.metrica.to.lower()
        punto_corte = metricas[metricas[metrica] <= self.kpi].threshold.values[0]

        new_labels = [0 if score < punto_corte else 1 for score in predicted_scores[:, 1]]

        aequitas_df = clean_data.iloc[X_test.index,]

        aequitas_df = pd.concat([aequitas_df[list(REF_GROUPS_DICT.keys())], aequitas_df['label']], axis=1)
        aequitas_df = aequitas_df.rename(columns={'label': 'label_value'})

        aequitas_df['score'] = new_labels

        tuplas = aequitas_df.to_records(index=False)
        for element in tuplas:
            yield element
