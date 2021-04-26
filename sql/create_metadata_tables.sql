set time zone  'America/Mexico_City';

--schema metadata
DROP SCHEMA IF EXISTS metadata CASCADE;
CREATE SCHEMA metadata;

DROP TABLE IF EXISTS metadata.feature_engineering;

CREATE TABLE metadata.feature_engineering(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL,
    tiempo varchar DEFAULT NULL,
    num_registros varchar DEFAULT NULL,
);

-- preprocessing
DROP TABLE IF EXISTS metadata.preprocessing;

CREATE TABLE metadata.preprocessing(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL,
    tiempo varchar DEFAULT NULL,
    num_registros varchar DEFAULT NULL,
);

-- almacenamiento
DROP TABLE IF EXISTS metadata.almacenamiento;

CREATE TABLE metadata.almacenamiento(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL
);

-- ingesta
DROP TABLE IF EXISTS metadata.ingesta;

CREATE TABLE metadata.ingesta(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL
);
