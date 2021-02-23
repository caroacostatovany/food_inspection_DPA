from src.pipeline.ingesta_almacenamiento import ingesta_inicial, get_client, ingesta_consecutiva
from datetime import date

cliente=get_client()
# Probamos para guardar la ingesta inicial
ingesta_inicial(cliente)

# Probamos para guardar la ingesta consecutiva
fecha=date.today()
ingesta_consecutiva(cliente, fecha)

