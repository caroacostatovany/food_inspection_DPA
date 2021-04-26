"""
Módulo para pruebas unitarias de ingesta
"""

import unittest
import marbles.core
import os
from unittest.mock import patch, Mock


class TestIngesta(marbles.core.TestCase):

    def test_ingesta(self, filepath):
        """Revisa que el archivo pese más de 1KB"""

        filesize = os.path.getsize(filepath)
        return self.assertGreater(filesize, 1000)
