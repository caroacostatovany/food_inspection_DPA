# Food inspection 

Los colaboradores en este proyecto somos:

| Nombre | usuario github |
|-------|-----------------|
| Cecilia Avilés | cecyar |
| Leonardo Ceja | lecepe00 |
| Eduado Moreno | Eduardo-Moreno|
| Carolina Acosta | caroacostatovany |

## Pregunta analítica a contestar con el modelo predictivo

¿El establecimiento pasará o no la inspección?

# Base de datos
La base de datos que se analizará en este trabajo será la de [Chicago food inspection](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5)

También se puede encontrar en nuestro [_Kaggle_](https://www.kaggle.com/carotovany/food-inspections), de acuerdo a la frecuencia de actualización del producto de datos, que será semanal.

### Resumen

- Al día 15 de enero del 2021 contamos con 215,067 registros

#### Columnas

| Nombre de la columna | Descripción | Tipo de variable | Registros únicos (15 enero) |
|----------------------|--------------|-----------------|------------------|
|Inspection ID | ID de la inspección realizada|texto | 215,067 |
|DBA Name| Nombre del establecimiento |texto | 28,748|
|AKA Name| (As known as) Nombre común del establecimiento |texto | 27,356 |
|License #| Número de la licencia del inspector |texto | 39,103 |
|Facility Type| Tipo de establecimiento |categórico (texto) | 501 |
|Risk| Tipo de Riesgo |categórico (numérico-texto) | 5 |
|Address| Dirección del establecimiento |texto | 18,522 |
|City| Ciudad en la que se encuentra el establecimiento |categórico(texto) | 71 |
|State| Estado en el que se encuentra el establecimiento |categrico(texto) | 5 |
|Zip| Código Postal del establecimiento |categorico(numérico) | 112 |
|Inspection Date| Fecha en la que se realizó la inspección |fecha | 2,796 |
|Inspection Type| Tipo de inspección que se realizó |categórico (texto)| 111 |
|Results* | Resultado de la inspección |categórico(texto) | 7 |
|Violations| Violaciones que ha realizado el establecimiento |texto | 156,639 |
|Latitude| Latitud |numérico | 17,246  |
|Longitude| Longitud |numérico | 17,246 |
|Location| Latitud y longitud |vector numérico | 17,247 |

*Variable target. 

### Frecuencia de actualización de los datos

La frecuencia de actualización de los datos en la base de datos original es diaria, la del producto de datos para este proyecto será semanal.

# Lenguaje de programación

Python 3.7.4

Se agregó un archivo `.python-version` que indica que nuestro ambiente virtual se llama `food-inspection`.

# EDA
Podrás encontrar nuestro notebook del Análisis Exploratorio en la siguiente ruta:

+ `notebooks/eda/EDA_GEDA.ipynb`

# Ejecución

1. Crea un ambiente virtual llamado `food-inspection` , actívalo e instala los `requirements.txt` con el siguiente comando:
> `pip install -r requirements.txt`

2. En la terminal, posiciónate en la raíz del repositorio y ejecuta:
>  `export PYTHONPATH=$PWD`

### De Notebooks

1. En la carpeta `data`, coloca el archivo `Food_Inspections.csv`
2. En la terminal, (una vez que hayas hecho todo lo anterior, instalar requirements y cargar la raíz como parte del PYTHONPATH) posiciónate en la raíz y ejecuta:
> `jupyter notebook`

