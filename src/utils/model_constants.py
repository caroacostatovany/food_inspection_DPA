from sklearn.ensemble import RandomForestClassifier
from sklearn.tree import DecisionTreeClassifier
from sklearn.linear_model import LogisticRegression

ALGORITHMS = ['tree', 'random_forest', 'logistic_regression']


GRID_SEARCH_DICT = {'tree': {'max_depth': [None],
                             'min_samples_leaf': [7]},

                    'random_forest': {'n_estimators': [30],
                                      'max_depth': [5],
                                      'min_samples_leaf': [3]},

                    'logistic_regression': {'penalty': ['l1'],
                                            'C': [0.01]}}

ESTIMATORS_DICT = {'tree': DecisionTreeClassifier(random_state=1111),
                   'random_forest': RandomForestClassifier(oob_score=True, random_state=2222),
                   'logistic_regression': LogisticRegression(random_state=3333)}