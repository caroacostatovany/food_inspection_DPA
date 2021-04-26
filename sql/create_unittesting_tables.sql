set time zone  'America/Mexico_City';

--schema metadata
DROP SCHEMA IF EXISTS test CASCADE;
CREATE SCHEMA test;

DROP TABLE IF EXISTS test.unit_testing;

CREATE TABLE test.unit_testing(
    user_id varchar DEFAULT NULL,
    parametros varchar DEFAULT NULL,
    task varchar DEFAULT NULL
);
