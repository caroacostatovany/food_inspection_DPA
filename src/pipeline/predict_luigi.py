import logging
import pandas as pd
import luigi
from datetime import date, datetime

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3, get_s3_resource
from src.utils.constants import S3, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS, NOMBRE_FE_predict, PATH_PREDICT, \
    NOMBRE_PREDICT, PATH_FE, NOMBRE_FE_xtest, NOMBRE_FE_predict, PATH_METRICAS, NOMBRE_METRICAS, NOMBRE_PREDICT_PROBAS
from src.pipeline.training_luigi import TaskTrainingMetadata
from src.pipeline.metricas_luigi import TaskMetricas
from src.unit_testing.test_predict import TestPredict
from src.pipeline.feature_engineering_luigi import TaskFeatureEngineeringMetadata
from src.etl.model_select import best_model_selection

logging.basicConfig(level=logging.INFO)


class TaskPredictUnitTesting(CopyToTable):

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

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "test.unit_testing"

    columns = [("user_id", "varchar"),
               ("modulo", "varchar"),
               ("prueba", "varchar"),
               ("dia_ejecucion", "timestamp without time zone")]

    def requires(self):
        return [TaskPredict(self.ingesta, self.fecha, self.fecha_modelo, self.threshold, self.algoritmo, self.metrica,
                            self.kpi)]

    def rows(self):

        path_s3 = PATH_PREDICT.format(self.fecha.year, self.fecha.month)
        filename = "{}/{}".format(path_s3, NOMBRE_PREDICT.format(self.fecha))
        predict_df = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        filename = "{}/{}".format(path_s3, NOMBRE_PREDICT_PROBAS.format(self.fecha))
        predicted_probas = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        unit_testing = TestPredict()
        unit_testing.test_predict_month(predict_df)
        unit_testing.test_predict_new_labels(predict_df)
        unit_testing.test_predict_probas(predicted_probas)

        #if not self.permite_nulos:
        #    metricas = obtain_metricas_sesgo_dataframe()
        #    unit_testing.test_sesgo_not_nan(metricas)

        r = [(self.user, "predict", "test_predict_month", datetime.now()),
             (self.user, "predict", "test_predict_new_labels", datetime.now()),
             (self.user, "predict", "test_predict_probas", datetime.now())]

        for element in r:
            yield element


class TaskPredictMetadata(CopyToTable):

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

    cred = get_db(CREDENCIALES)
    user = cred['user']
    password = cred['pass']
    database = cred['db']
    host = cred['host']
    port = cred['port']

    table = "metadata.predict"

    columns = [("user_id", "varchar"),
               ("parametros", "varchar"),
               ("dia_ejecucion", "timestamp without time zone")]

    def requires(self):
        return TaskPredictUnitTesting(self.ingesta, self.fecha, self.fecha_modelo, self.threshold, self.algoritmo,
                                      self.metrica, self.kpi)

    def rows(self):
        param = "{0}; {1}; {2}; {3}; {4}; {5}; {6}; {7}".format(self.ingesta,
                                                                self.fecha,
                                                                self.fecha_modelo,
                                                                self.threshold,
                                                                self.algoritmo,
                                                                self.metrica,
                                                                self.kpi,
                                                                self.permite_nulos)
        r = [(self.user, param, datetime.now())]
        for element in r:
            yield element


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
        filename = "{}/{}".format(path_s3, NOMBRE_METRICAS.format(self.fecha_modelo))
        metricas = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        # Leemos predict
        path_s3 = PATH_FE.format(self.fecha.year, self.fecha.month)
        file_predict = NOMBRE_FE_predict.format(self.fecha)
        filename = "{}/{}".format(path_s3, file_predict)
        predictions_df = read_pkl_from_s3(S3, BUCKET_NAME, filename)
        predictions_df = predictions_df.drop('label', axis=1)

        metrica = self.metrica.lower()
        punto_corte = metricas[metricas[metrica] <= self.kpi].threshold.values[0]

        for col in X_test.columns:
            if col in predictions_df.columns:
                pass
            else:
                predictions_df[col] = 0

        predicted_scores = model.predict_proba(predictions_df.drop('label', axis=1))
        labels = [0 if score < punto_corte else 1 for score in predicted_scores[:, 1]]
        predictions_df['predicted_labels'] = labels

        path_s3 = PATH_PREDICT.format(self.fecha.year, self.fecha.month)
        filename = "{}/{}".format(path_s3, NOMBRE_PREDICT.format(self.fecha))
        guardar_pkl_en_s3(S3, BUCKET_NAME, filename, predictions_df)

        filename = "{}/{}".format(path_s3, NOMBRE_PREDICT_PROBAS.format(self.fecha))
        guardar_pkl_en_s3(S3, BUCKET_NAME, filename, predicted_scores)

    def output(self):
        #
        path_s3 = PATH_PREDICT.format(self.fecha.year, self.fecha.month)
        file_to_upload_predict = NOMBRE_PREDICT.format(self.fecha)
        output_path_predict = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                     path_s3,
                                                     file_to_upload_predict)

        file_to_upload_predict_probas = NOMBRE_PREDICT_PROBAS.format(self.fecha)
        output_path_predict_probas = "s3://{}/{}/{}".format(BUCKET_NAME,
                                                            path_s3,
                                                            file_to_upload_predict_probas)

        return [luigi.contrib.s3.S3Target(path=output_path_predict),
                luigi.contrib.s3.S3Target(path=output_path_predict_probas)]
