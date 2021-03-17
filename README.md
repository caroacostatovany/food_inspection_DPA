![](https://mcdatos.itam.mx/wp-content/uploads/2020/11/ITAM-LOGO.03.jpg)
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
La base de datos que se analizará en este trabajo será la de [Chicago food inspection](https://data.cityofchicago.org/Health-Human-Services/Food-Inspections/4ijn-s7e5).

También se puede encontrar en nuestro [_Kaggle_](https://www.kaggle.com/carotovany/food-inspections), de acuerdo a la frecuencia de actualización del producto de datos, que será semanal.

### Resumen

- Al día 15 de enero del 2021 contamos con 215,067 registros.

#### Columnas

| Nombre de la columna | Descripción | Tipo de variable | Registros únicos (15 enero) |
|----------------------|--------------|-----------------|------------------|
|Inspection ID | ID de la inspección realizada|texto | 215,067 |
|DBA Name| Nombre del establecimiento |texto | 28,748|
|AKA Name| (Also known as) Nombre común del establecimiento |texto | 27,356 |
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

1. Crea un ambiente virtual llamado `food-inspection`, actívalo e instala los `requirements.txt` con el siguiente comando:
> `pip install -r requirements.txt`

2. En la terminal, posiciónate en la raíz del repositorio y ejecuta:
>  `export PYTHONPATH=$PWD`


### De Funciones

1. Una  vez creado el ambiente virtual y habiendo agregado el proyecto como variable de PYTHONPATH, puedes hacer uso de las funciones descritas en cada archivo.

**Notas:** en `src/utils/constants.py` mantenemos las constantes de nuestro proyecto, donde tenemos referenciado el nombre de nuestro bucket: ` "data-product-architecture-equipo-3"`  y la ruta de las credenciales para acceder al bucket (`"../conf/local/credentials.yaml"`).   
Si quieres acceder a diferentes buckets con otras credenciales esto se deberá cambiar en el archivo de las constantes.

#### Ejemplo de script para ingesta inicial:

    from src.pipeline.ingesta_almacenamiento import ingesta_inicial, get_client
    from datetime import date
    
    # Nos conectamos a data.cityofchicago.org a través de Socrata
    cliente=get_client()
    
    # Hacemos la ingesta inicial
    ingesta_inicial(cliente) # El límite está por default a 300,000. En dado caso que se quiera cambiar, debe ser ingesta_inicial(cliente, limite=<nuevo_limite>)

#### Ejemplo de script para ingesta consecutiva:

    from src.pipeline.ingesta_almacenamiento import ingesta_consecutiva, get_client
    from datetime import date
    
    # Nos conectamos a data.cityofchicago.org a través de Socrata
    cliente=get_client()

    fecha=date.today()

    # Hacemos la ingesta consecutiva
    ingesta_consecutiva(cliente, fecha) # El límite está por default a 1,000. En dado caso que se quiera cambiar, debe ser ingesta_consecutiva(cliente, fecha, limite=<nuevo_limite>)


### De Notebooks

1. En la carpeta `data`, coloca el archivo `Food_Inspections.csv`.
2. En la terminal, (una vez que hayas hecho todo lo anterior, instalar requirements y cargar la raíz como parte del PYTHONPATH) posiciónate en la raíz y ejecuta:
> `jupyter notebook`

### Con Luigi

Sólo existen los parámetros --ingesta-inicial (que es booleano, si se escribe será Verdadero), --ingesta-consecutiva(que también es booleano) y --fecha (que se refiere a la fecha en la que se corre el proceso, si se omite será la de hoy, el formato esta en año-mes-día)

Para ingesta inicial
>  PYTHONPATH=$PWD AWS_PROFILE=<tu_profile_en_aws_config> luigi --module src.pipeline.almacenamiento_luigi TaskAlmacenamiento --ingesta-inicial

Para ingesta consecutiva
> PYTHONPATH=$PWD AWS_PROFILE=<tu_profile_en_aws_config> luigi --module src.pipeline.almacenamiento_luigi TaskAlmacenamiento  --ingesta-consecutiva --fecha 2020-11-03

### Sobre tus credenciales

En la carpeta `conf/local` deberás colocar tu archivo `credentials.yaml`. La estructura del mismo debe ser la siguiente:
```
s3:
    aws_access_key_id: "tuaccesskeyid"
    aws_secret_access_key: "tusecretaccesskey"
food_inspections:
    app_token: "tutoken"
    username: "tuusername"
    password: "tucontraseña"
```

Para poder ejecutar Luigi, se deberá modificar el archivo de credenciales de AWS(~/.aws/credentials) y deberá tener la estructura siguiente:
```
[tu_profile]
    aws_access_key_id = tuaccesskeyid
    aws_secret_access_key = tusecretaccesskey
    region = tu-region
```

## FAQ
### ¿Qué hace el proceso de ingestión inicial?
**R:** La función de `ingesta_inicial` utiliza el cliente, que se conectó previamente a **data.cityofchicago.org** a través de *Socrata* y un *token*, para obtener datos del dataset: **Food inspections (ID: 4ijn-s7e5)** con un límite de *data points* por *default* de 300,000. Una vez obtenidos, se guardan en un bucket de AWS especificado en `constants.py`, en el path: `ingestion/inital` bajo el nombre de `historic-inspections-{dia_de_hoy}.pkl`.

### ¿Qué hace el proceso de ingestión consecutiva?
**R:** La función de `ingesta_consecutiva` utiliza el cliente, que se conectó previamente a **data.cityofchicago.org** a través de *Socrata* y un *token*, y una fecha para obtener datos del dataset: **Food inspections (ID: 4ijn-s7e5)** con un límite de *data points* por *default* de 1,000 y desde 7 días antes a la fecha especificada hasta la fecha especificada en la variable `fecha`. Una vez obtenidos, se guardan en un bucket de AWS especificado en `constants.py`, en el path: `ingestion/consecutive` bajo el nombre de `consecutive-inspections-{fecha_hoy}.pkl`.



