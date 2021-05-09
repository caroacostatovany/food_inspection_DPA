"""
Módulo para pruebas unitarias de sesgo e inequidad
"""
import unittest
import marbles.core
from marbles.mixins import mixins
from unittest.mock import patch, Mock

from src.etl.preprocessing import preprocessing


class TestSesgoInequidad(marbles.core.TestCase, mixins.CategoricalMixins):

    labels = [0, 1]

    def test_sesgo_score(self, df):
        """Revisa que el score sea 0 ó 1"""
        self.assertCategoricalLevelsEqual(df['score'], self.labels,
                                          note="La columna score sólo puede contener valores de 0 y 1")

    def test_sesgo_label_value(self, df):
        """Revisa la etiqueta sea 0 ó 1"""
        self.assertCategoricalLevelsEqual(df['label_value'], self.labels,
                                          note="La columna label_value sólo puede contener valores de 0 y 1")

    def test_sesgo_not_nan(self, df):
        """Revisa que el dataframe no tenga nulos"""
        self.assertTrue(df.isnull().values.any(), note="Las métricas de sesgo e inequidad contienen nulos")
