"""
Módulo para pruebas unitarias de feature engineering
"""
import unittest
import marbles.core

from unittest.mock import patch, Mock
from src.etl.feature_engineering import feature_generation


class TestFeatureEngineering(marbles.core.TestCase):

    def test_feature_engineering(self, df):
        """Revisa que no haya nulos"""

        self.feature_df = feature_generation(df)
        return self.assertIsNotNone(self.feature_df)
