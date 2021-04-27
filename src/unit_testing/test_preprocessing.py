"""
Módulo para pruebas unitarias de preprocessing
"""
import unittest
import marbles.core
from marbles.mixins import mixins
from unittest.mock import patch, Mock

from src.etl.preprocessing import preprocessing


class TestPreprocessing(marbles.core.TestCase, mixins.BetweenMixins):

    def test_preprocessing(self):
        """Revisa la etiqueta sea 0 ó 1"""

        self.food_df = preprocessing()
        label = self.food_df['label']
        return self.assertBetween(label, lower=0, upper=1)
