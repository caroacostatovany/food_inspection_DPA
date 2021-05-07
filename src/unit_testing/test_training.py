"""
Módulo para pruebas unitarias de entrenamiento
"""
import unittest
import marbles.core
import json

from unittest.mock import patch, Mock
from src.utils.general import read_pkl_from_s3, get_s3_resource
from src.utils.constants import CREDENCIALES, BUCKET_NAME

import sklearn
from sklearn.model_selection import GridSearchCV

class TestTraining(marbles.core.TestCase):

    def test_training_gs(self, file):
        """Revisa que el archivo es un objeto GridSearchCV"""
        # Crear objeto dummy
        #dummyGS = GridSearchCV()

        # Realmente esta definido como un diccionario y por eso usamos dumps, en vez de loads
        self.assertTrue(isinstance(file, sklearn.model_selection._search.GridSearchCV), note="El archivo debe ser de tipo GridSearchCV")
