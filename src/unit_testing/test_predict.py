"""
Módulo para pruebas unitarias de predict
"""
import unittest
import marbles.core

from marbles.mixins import mixins
from unittest.mock import patch, Mock


class TestPredict(marbles.core.TestCase, mixins.BetweenMixins, mixins.CategoricalMixins):

    labels = [0, 1]

    def test_predict_new_labels(self, df):
        """Revisa la etiqueta sea 0 ó 1"""
        self.assertCategoricalLevelsEqual(df['predicted_labels'], self.labels,
                                          note="La columna predicted_labels sólo puede contener valores de 0 y 1")

    def test_predict_probas(self, array):
        """Revisa si las probabilidades van de 0 a 1"""
        self.assertBetween(array.all(), strict=False, lower=0, upper=1, note= "Las probas deben ir entre 0 y 1")

    def test_predict_probas_strict(self, array):
        """Revisa que las probabilidades sean estrictamente debajo de 100"""
        self.assertBetween(array.all(), strict=True, lower=0, upper=1,
                           note="La probas deben ser estrictamente arriba de 0 y debajo de 1")
