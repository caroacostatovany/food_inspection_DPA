from src.utils.constants import BUCKET_NAME
from src.utils.general import read_pkl_from_s3

import logging

logging.basicConfig(level=logging.INFO)

def best_model_selection(threshold, objects, s3):
    """
    Selección del mejor modelo de acuerdo al criterio del cliente
    :param threshold: criterio del cliente
    :return: mejor modelo
    """

    best_model = ''
    best_score = ''
    max_score = 0


    # Leyendo modelos
    if len(objects) > 0:
        for file in objects:
            if file['Key'].find("models/") >= 0:
                filename = file['Key']
                logging.info("Leyendo {}...".format(filename))
                json_file = read_pkl_from_s3(s3, BUCKET_NAME, filename)
                loaded_model = json_file
                if loaded_model.best_score_ >= threshold:
                    if loaded_model.best_score_ >= max_score:
                        best_model = filename
                        best_score = loaded_model.best_score_
                        max_score = best_score

    return best_model, best_score
