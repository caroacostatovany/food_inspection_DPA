set time zone  'America/Mexico_City';

set role postgres; -- cambiarlo

--schema raw
DROP SCHEMA IF EXISTS raw CASCADE;

CREATE SCHEMA raw;

--table food inspection
DROP TABLE IF EXISTS raw.food_inspection;

CREATE TABLE raw.food_inspection(
    inspection_id varchar,
    dba_name varchar,
    aka_name varchar,
    license_ varchar,
    facility_type varchar,
    risk varchar,
    address varchar,
    city varchar,
    state varchar,
    inspection_date varchar,
    inspection_type varchar,
    results varchar,
    latitude varchar,
    longitude varchar,
    location json
);
