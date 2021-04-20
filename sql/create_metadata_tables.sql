set time zone  'America/Mexico_City';

--schema metadata
DROP SCHEMA IF EXISTS metadata CASCADE;
CREATE SCHEMA metadata;

DROP TABLE IF EXISTS metadata.feature_engineering;
DROP TABLE IF EXISTS metadata.preprocessing;

CREATE TABLE metadata.feature_engineering(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL,
    tiempo varchar DEFAULT NULL,
    num_registros varchar DEFAULT NULL,
);

CREATE TABLE metadata.preprocessing(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    dia_ejecucion varchar DEFAULT NULL,
    tiempo varchar DEFAULT NULL,
    num_registros varchar DEFAULT NULL,
);