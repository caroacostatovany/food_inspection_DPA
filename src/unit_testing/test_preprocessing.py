"""
Módulo para pruebas unitarias de preprocessing
"""
import unittest
import marbles.core
from marbles.mixins import mixins
from unittest.mock import patch, Mock

from src.etl.preprocessing import preprocessing


class TestPreprocessing(marbles.core.TestCase, mixins.CategoricalMixins):

    labels = [0, 1]

    def test_preprocessing_label(self, df):
        """Revisa la etiqueta sea 0 ó 1"""
        self.assertCategoricalLevelsEqual(df['label'], self.labels)
