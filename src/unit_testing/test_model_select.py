"""
Módulo para pruebas unitarias de ingesta
"""

import unittest
import marbles.core
import os
from unittest.mock import patch, Mock


class TestModelSelect(marbles.core.TestCase):

    def test_model_select(self, modelo):
        """Revisa que el modelo sea distinto a la cadena vacia que indica que no hubo mejor modelo"""

        return self.assertNotEqual(modelo, '')
