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

También se puede encontrar en nuestro [_Kaggle_](https://www.kaggle.com/carotovany/food-inspections), de acuerdo a la frecuencia de actualización del procuto de datos, que será semanal.

### Resumen

- Al día 15 de enero contamos con 215,067 registros

#### Columnas

| Nombre de la columna | Tipo de variable | Registros únicos |
|----------------------|------------------|------------------|
|Inspection ID | numérico | 215,067 |
|DBA Name| texto | 28,748|
|AKA Name| texto | 27,356 |
|License #| numérico | 39,103 |
|Facility Type| categórico (texto) | 501 |
|Risk| categórico (numérico-texto) | 5 |
|Address| texto | 18,522 |
|City| categórico(texto)? | 71 |
|State| categórico(texto)? | 5 |
|Zip| numérico | 112 |
|Inspection Date| fecha | 2,796 |
|Inspection Type| categórico (texto)| 111 |
|Results* | categórico(texto) | 7 |
|Violations| texto | 156,639 |
|Latitude| numérico | 17,246  |
|Longitude| numérico | 17,246 |
|Location| vector numérico | 17,247 |

*Variable target. 

### Frecuencia de actualización de los datos

La frecuencia de actualización de los datos en la base de datos original es diaria, la del producto de datos para este proyecto será semanal.

### Lenguaje de programación

Python 3.7.4
