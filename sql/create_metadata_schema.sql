set time zone  'America/Mexico_City';

--set role tovany; -- cambiarlo

--schema raw
DROP SCHEMA IF EXISTS metadata CASCADE;

CREATE SCHEMA metadata;

--table json2rds
DROP TABLE IF EXISTS metadata.json2rds;

CREATE TABLE metadata.json2rds(
    user_id varchar,
    metadata json
);

--table metadata preprocessing
DROP TABLE IF EXISTS metadata.preprocessing;

CREATE TABLE metadata.preprocessing(
    user_id varchar,
    metadata json
);
