import logging
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
import pandas as pd
from src.utils.model_constants import ESTIMATORS_DICT, GRID_SEARCH_DICT

import time

logging.basicConfig(level=logging.INFO)


def fit_training_food(X_train, y_train, algorithm):
    """
    Entrenamiento de algoritmo
    :df: dataframe
    :best_model: mejor modelo
    """

    # Empezar proceso
    logging.info("Model selection processing...")

    estimator = ESTIMATORS_DICT[algorithm]
    grid_params = GRID_SEARCH_DICT[algorithm]

    ### Time Split (Cross-Validation)
    tscv = TimeSeriesSplit(n_splits=5)
    gs = GridSearchCV(estimator, grid_params, scoring='precision', cv=tscv, n_jobs=3)

    # train
    print("!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!!", y_train.shape)
    gs.fit(X_train, y_train)

    return gs
