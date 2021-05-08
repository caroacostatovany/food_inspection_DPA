import scipy
import numpy as np
import itertools
import logging
import pandas as pd
import luigi
from datetime import date

from sklearn.preprocessing import OneHotEncoder
from luigi.contrib.s3 import S3Target
from luigi.contrib.postgres import CopyToTable

from src.utils.general import get_db, read_pkl_from_s3, guardar_pkl_en_s3, get_s3_resource
from src.utils.constants import S3, CREDENCIALES, BUCKET_NAME, PATH_MS, NOMBRE_MS, PATH_FE, NOMBRE_FE_xtest, NOMBRE_FE_ytest, REF_GROUPS_DICT, PATH_PREPROCESS, NOMBRE_PREPROCESS
from src.pipeline.model_select_luigi import TaskModelSelectionMetadata
from src.etl.metricas import get_metrics_matrix

logging.basicConfig(level=logging.INFO)


def reverse_one_hot(X, y, encoder):
    reversed_data = [{} for _ in range(len(y))]
    all_categories = list(itertools.chain(*encoder.categories))
    category_names = ['category_{}'.format(i+1) for i in range(len(encoder.categories))]
    category_lengths = [len(encoder.categories[i]) for i in range(len(encoder.categories))]

    for row_index,feature_index in zip(X):
        print("##########################################",row_index)
        category_value = all_categories[feature_index]
        #category_name = get_category_name(feature_index, category_names, category_lengths)
        #reversed_data[row_index][category_name] = category_value
        #reversed_data[row_index]['target'] = y[row_index]

    return reversed_data


def get_category_name(index, names, lengths):

    counter = 0
    for i in range(len(lengths)):
        counter += lengths[i]
        if index < counter:
            return names[i]
    raise ValueError('The index is higher than the number of categorical values')


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

        file_ytest = NOMBRE_FE_ytest.format(self.fecha)
        filename = "{}/{}".format(path_s3, file_ytest)
        y_test = read_pkl_from_s3(S3, BUCKET_NAME, filename)

        # Leer el mejor modelo
        path_s3 = PATH_MS.format(self.fecha.year, self.fecha.month)
        filename = path_s3 + "/" + NOMBRE_MS.format(self.fecha)
        best_model = read_pkl_from_s3(S3, BUCKET_NAME, filename)
        model = read_pkl_from_s3(S3, BUCKET_NAME, best_model)

        predicted_scores = model.predict_proba(X_test)

        path_s3 = PATH_PREPROCESS.format(self.fecha.year, self.fecha.month)
        preprocess_file = "{}/{}".format(path_s3, NOMBRE_PREPROCESS.format(self.fecha))
        clean_data = read_pkl_from_s3(S3, BUCKET_NAME, preprocess_file)

        metricas = get_metrics_matrix(y_test, predicted_scores)

        metrica = self.metrica.lower()
        punto_corte = metricas[metricas[metrica] <= self.kpi].threshold.values[0]

        new_labels = [0 if score < punto_corte else 1 for score in predicted_scores[:, 1]]

        ohc=OneHotEncoder()
        #clean_X = reverse_one_hot(X_test, y_test, ohe)
        X = X_test.to_records(index=False)
        #print(clean_X)

        n_samples, n_features = X.shape()
        n_values = np.max(X, axis=0) + 1
        self.n_values_ = "auto"

        n_values = np.hstack([[0], n_values])
        indices = np.cumsum(n_values)
        self.feature_indices_ = indices

        column_indices = (X + indices[:-1]).ravel()

        row_indices = np.repeat(np.arange(n_samples, dtype=np.int32), n_features)

        data = np.ones(n_samples * n_features)

        out = sparse.coo_matrix((data, (row_indices, column_indices)),
                        shape=(n_samples, indices[-1]),
                        dtype=self.dtype).tocsr()

        if self.n_values == 'auto':
            mask = np.array(out.sum(axis=0)).ravel() != 0
            active_features = np.where(mask)[0]  # array([  3,  10,  15,  33,  54,  55,  78,  79,  80,  99, 101, 103, 105, 107, 108, 112, 115, 119, 120])
            out = out[:, active_features]  # <10x19 sparse matrix of type '<type 'numpy.float64'>' with 20 stored elements in Compressed Sparse Row format>
            self.active_features_ = active_features

            return out if self.sparse else out.toarray()

        out = out.sorted_indices()
        #out = scipy.sparse.csr_matrix(X_test.values)
        decode_columns = np.vectorize(lambda col: ohc.active_features_[col])
        decoded = decode_columns(out.indices).reshape(out.shape)
        recovered_X = decoded - ohc.feature_indices_[:-1]

        print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!",recovered_X)
        print("*******************************************************************",X_test.index)
        aequitas_df = clean_data.iloc[X_test.index,]

        aequitas_df = pd.concat([aequitas_df[list(REF_GROUPS_DICT.keys())], aequitas_df['label']], axis=1)
        aequitas_df = aequitas_df.rename(columns={'label': 'label_value'})

        aequitas_df['score'] = new_labels

        tuplas = aequitas_df.to_records(index=False)
        for element in tuplas:
            yield element
