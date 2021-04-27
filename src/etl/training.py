import logging
from sklearn.model_selection import GridSearchCV, TimeSeriesSplit
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression
import pandas as pd
import time

logging.basicConfig(level=logging.INFO)

def magic_loop(X_train, y_train):
    """
    Evaluación de metodología Magic Loop en la etapa de modelling
    :df: dataframe
    :best_model: mejor modelo
    """

    # Magic Loop
    algorithms = ['tree', 'random_forest', 'logistic_regression']
    algorithms_dict = {'tree': 'tree_grid_search',
                       'random_forest': 'rf_grid_search',
                       'logistic_regression': 'lr_grid_search'}
    grid_search_dict = {'tree_grid_search': {'max_depth': [5, 10, 15, None],
                                             'min_samples_leaf': [3, 5, 7]},
                        'rf_grid_search': {'n_estimators': [30, 50, 100],
                                           'max_depth': [5, 10],
                                           'min_samples_leaf': [3, 5]},
                        'lr_grid_search': {'penalty': ['l1', 'l2'],
                                           'C': [0.01, 1, 100, 1000]}}

    estimators_dict = {'tree': DecisionTreeClassifier(random_state=1111),
                       'random_forest': RandomForestClassifier(oob_score=True, random_state=2222),
                       'logistic_regression': LogisticRegression(random_state=3333)}

    # Empezar proceso
    best_estimators = []
    start_time = time.time()
    logging.info("Model selection processing...")
    for algorithm in algorithms:
        estimator = estimators_dict[algorithm]
        grid_search_to_look = algorithms_dict[algorithm]
        grid_params = grid_search_dict[grid_search_to_look]

        ### Time Split (Cross-Validation)
        tscv = TimeSeriesSplit(n_splits=5)
        gs = GridSearchCV(estimator, grid_params, scoring='precision', cv=tscv, n_jobs=3)

        # train
        gs.fit(X_train, y_train)

        # best estimator
        best_estimators.append(gs)

    logging.info("Model selection succesfully...")
    print("Tiempo de ejecución: ", time.time() - start_time)

    # Seleccionar el de mejor desempeño
    models_develop = [best_estimators[0].best_score_, best_estimators[1].best_score_, best_estimators[2].best_score_]
    max_score = max(models_develop)
    max_index = models_develop.index(max_score)
    best_model = best_estimators[max_index]

    print('Mejor modelo: ', best_model.best_estimator_)
    print('Mejor desempeño: ', best_model.best_score_)
    return best_model
