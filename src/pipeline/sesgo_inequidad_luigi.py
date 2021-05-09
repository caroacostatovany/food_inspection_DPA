import logging
import pandas as pd
import luigi
import aequitas

from datetime import date, datetime

from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable
from aequitas.group import Group
from aequitas.bias import Bias
from aequitas.fairness import Fairness

from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3, get_s3_resource
from src.utils.constants import S3, CREDENCIALES, BUCKET_NAME, REF_GROUPS_DICT, PATH_MS, NOMBRE_MS, PATH_FE, \
    NOMBRE_FE_xtest, NOMBRE_FE_ytest
from src.pipeline.metricas_luigi import TaskMetricas
from src.etl.metricas import get_metrics_matrix
from src.etl.sesgo_inequidad import obtain_aequitas_dataframe, obtain_metricas_sesgo_dataframe
from src.unit_testing.test_sesgo_inequidad import TestSesgoInequidad

logging.basicConfig(level=logging.INFO)


class TaskSesgoInequidadUnitTesting(CopyToTable):
    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    threshold = luigi.FloatParameter(default=0.80, description="Umbral del desempeño para escoger el mejor modelo")

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

    permite_nulos = luigi.BoolParameter(default=True, description="Permite nulos en métricas de sesgo e inequidad")

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
        return [TaskSesgoInequidad(self.ingesta, self.fecha, self.threshold, self.algoritmo, self.metrica, self.kpi)]

    def rows(self):
        aequitas_df = obtain_aequitas_dataframe()

        unit_testing = TestSesgoInequidad()
        unit_testing.test_sesgo_score(aequitas_df)
        unit_testing.test_sesgo_label_value(aequitas_df)
        if not self.permite_nulos:
            metricas = obtain_metricas_sesgo_dataframe()
            unit_testing.test_sesgo_not_nan(metricas)

        r = [(self.user, "sesgo_inequidad", "test_sesgo_score", datetime.now()),
             (self.user, "sesgo_inequidad", "test_sesgo_label_value", datetime.now())]
        for element in r:
            yield element


class TaskSesgoInequidad(CopyToTable):

    ingesta = luigi.Parameter(default="No", description="'No': si no quieres que corra ingesta. "
                                                        "'inicial': Para correr una ingesta inicial."
                                                        "'consecutiva': Para correr una ingesta consecutiva")

    fecha = luigi.DateParameter(default=date.today(), description="Fecha en que se ejecuta la acción. "
                                                                  "Formato 'año-mes-día'")

    threshold = luigi.FloatParameter(default=0.80, description="Umbral del desempeño para escoger el mejor modelo")

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

    table = "results.sesgo"
    columns = [('attribute_name', 'varchar'),
               ('attribute_value', 'varchar'),
               ('ppr_disparity', 'float'),
               ('pprev_disparity', 'float'),
               ('precision_disparity', 'float'),
               ('fdr_disparity', 'float'),
               ('for_disparity', 'float'),
               ('fpr_disparity', 'float'),
               ('fnr_disparity', 'float'),
               ('tpr_disparity', 'float'),
               ('tnr_disparity', 'float'),
               ('npv_disparity', 'float')]

    def requires(self):
        return [TaskMetricas(self.ingesta, self.fecha, self.threshold, self.algoritmo, self.metrica, self.kpi)]

    def rows(self):
        aequitas_df = obtain_aequitas_dataframe()

        g = Group()
        #xtab es la matriz que explica el comportamiento
        xtab, attrbs = g.get_crosstabs(aequitas_df)
        absolute_metrics = g.list_absolute_metrics(xtab)

        bias = Bias()

        bdf = bias.get_disparity_predefined_groups(xtab, original_df=aequitas_df,
                                                   ref_groups_dict=REF_GROUPS_DICT,
                                                   alpha=0.05)

        # View disparity metrics added to dataframe
        bias_matrix = bdf[['attribute_name', 'attribute_value'] + bias.list_disparities(bdf)].round(2)

        fair = Fairness()
        fdf = fair.get_group_value_fairness(bdf)
        parity_determinations = fair.list_parities(fdf)
        fairness_matrix = fdf[['attribute_name', 'attribute_value'] + absolute_metrics + bias.list_disparities(fdf) + parity_determinations].round(2)
        # Seleccionamos sólo las métricas que nos interesan

        tuplas_bias = bias_matrix.to_records(index=False)
        tuplas_fairness = fairness_matrix.to_records(index=False)

        for element in tuplas_bias:
            yield element

        #for element in tuplas_fairness:
        #    yield element
