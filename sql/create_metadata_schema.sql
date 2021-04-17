set time zone  'America/Mexico_City';

--set role tovany; -- cambiarlo

--schema raw
DROP SCHEMA IF EXISTS metadata CASCADE;

CREATE SCHEMA metadata;

--table food inspection
DROP TABLE IF EXISTS metadata.json2rds;

CREATE TABLE metadata.json2rds(
    user_id varchar,
    metadata json
);