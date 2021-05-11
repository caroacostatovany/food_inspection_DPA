set time zone  'America/Mexico_City';

--schema metadata
DROP SCHEMA IF EXISTS metadata CASCADE;
CREATE SCHEMA metadata;

--sesgo_inequidad
DROP TABLE IF EXISTS metadata.sesgo_inequidad;

CREATE TABLE metadata.sesgo_inequidad(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion timestamp without time zone DEFAULT NULL
);

--model_selection
DROP TABLE IF EXISTS metadata.model_selection;

CREATE TABLE metadata.model_selection(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion timestamp without time zone DEFAULT NULL
);

--training
DROP TABLE IF EXISTS metadata.training;

CREATE TABLE metadata.training(
    date varchar DEFAULT NULL,
    algorithm varchar DEFAULT NULL,
    best_params varchar DEFAULT NULL,
    X_train_file varchar DEFAULT NULL,
    y_train_file varchar DEFAULT NULL
);

--feature_engineering
DROP TABLE IF EXISTS metadata.feature_engineering;

CREATE TABLE metadata.feature_engineering(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL,
    tiempo varchar DEFAULT NULL,
    num_registros varchar DEFAULT NULL
);

-- preprocessing
DROP TABLE IF EXISTS metadata.preprocessing;

CREATE TABLE metadata.preprocessing(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL,
    tiempo varchar DEFAULT NULL,
    num_registros varchar DEFAULT NULL
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
