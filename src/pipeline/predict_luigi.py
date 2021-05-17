import logging
import pandas as pd
import luigi
from datetime import date, datetime

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3, get_s3_resource
from src.utils.constants import S3, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS
from src.pipeline.training_luigi import TaskTrainingMetadata
from src.unit_testing.test_model_select import TestModelSelect
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineeringMetadata
from src.etl.model_select import best_model_selection

logging.basicConfig(level=logging.INFO)


class TaskPredict(luigi.Task):

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

    def requires(self):
        return [TaskMetricas(self.ingesta, self.fecha_modelo, self.threshold, self.algoritmo, self.metrica, self.kpi),
                TaskFeatureEngineeringMetadata(self.ingesta, self.fecha, True)]  # True porque viene de predict

    def run(self):
        # Leemos x_test
        path_s3 = PATH_FE.format(self.fecha_modelo.year, self.fecha_modelo.month)
        file_xtest = NOMBRE_FE_xtest.format(self.fecha_modelo)
        filename = "{}/{}".format(path_s3, file_xtest)
        X_test = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        # Leer el mejor modelo
        path_s3 = PATH_MS.format(self.fecha_modelo.year, self.fecha_modelo.month)
        filename = path_s3 + "/" + NOMBRE_MS.format(self.fecha_modelo)
        best_model = read_pkl_from_s3(S3, BUCKET_NAME, filename)
        model = read_pkl_from_s3(S3, BUCKET_NAME, best_model)

        # Leer métricas
        path_s3 = PATH_METRICAS.format(self.fecha_modelo.year, self.fecha_modelo.month)
        filename = "{}/{}".format(path_s3, NOMBRE_METRICAS.format(self.fecha))
        metricas = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        # Leemos predict
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_xtest = NOMBRE_FE_predict.format(self.fecha)
        filename = "{}/{}".format(path_s3, file_xtest)
        predictions_df = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        metrica = self.metrica.lower()
        punto_corte = metricas[metricas[metrica] <= self.kpi].threshold.values[0]

        assert predictions_df.columns == X_test.columns

        predicted_scores = model.predict_proba(predictions_df)
        labels = [0 if score < punto_corte else 1 for score in predicted_scores[:, 1]]
        predictions_df['predicted_labels'] = labels

        path_s3 = PATH_PREDICT.format(self.fecha.year, self.fecha.month)
        filename = "{}/{}".format(path_s3, NOMBRE_PREDICT.format(self.fecha))
        guardar_pkl_en_s3(S3, BUCKET_NAME, filename, predictions_df)

    def output(self):
        #
        path_s3 = PATH_PREDICT.format(self.fecha.year, self.fecha.month)
        file_to_upload_predict = NOMBRE_PREDICT.format(self.fecha)
        output_path_predict = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                        path_s3,
                                                        file_to_upload_predict)

        return luigi.contrib.s3.S3Target(path=output_path_predict)
