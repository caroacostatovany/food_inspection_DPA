"""
Módulo para pruebas unitarias de feature engineering
"""
import unittest
import marbles.core


from marbles.mixins import mixins
from unittest.mock import patch, Mock
from src.etl.feature_engineering import feature_generation


class TestFeatureEngineering(marbles.core.TestCase, mixins.BetweenMixins):

    def test_feature_engineering_month(self, df):
        self.assertBetween(df['month'].any(), strict=False, lower=1, upper=12, note="Sólo puede haber meses entre 0 y 12")
