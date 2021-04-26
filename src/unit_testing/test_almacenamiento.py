"""
Módulo para pruebas unitarias de almacenamiento
"""
import unittest
import marbles.core
import json

from unittest.mock import patch, Mock
from src.etl.ingesta_almacenamiento import get_s3_resource
from src.utils.general import read_pkl_from_s3
from src.utils.constants import CREDENCIALES, BUCKET_NAME

class TestAlmacenamiento(marbles.core.TestCase):

    def test_almacenamiento_json(self, filename):
        """Revisa que el archivo es un json"""

        def is_json(myjson):
            try:
                json_object = json.loads(myjson)
            except ValueError as e:
                return False
            return True

        s3 = get_s3_resource(CREDENCIALES)
        file = read_pkl_from_s3(s3, BUCKET_NAME, filename)
        self.assertTrue(is_json(file))


