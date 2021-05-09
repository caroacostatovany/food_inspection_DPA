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
        """Revisa la etiqueta sea 0 ó 1"""
        self.assertCategoricalLevelsEqual(df['score'], self.labels, note="La etiqueta sólo puede contener valores de 0 y 1")

    def test_sesgo_label_value(self, df):
        """Revisa la etiqueta sea 0 ó 1"""
        self.assertCategoricalLevelsEqual(df['label_value'], self.labels, note="La etiqueta sólo puede contener valores de 0 y 1")
