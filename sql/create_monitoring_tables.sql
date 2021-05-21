set time zone  'America/Mexico_City';

--schema monitoring
DROP SCHEMA IF EXISTS monitoring CASCADE;
CREATE SCHEMA monitoring;

--scores
DROP TABLE IF EXISTS monitoring.scores;

CREATE TABLE monitoring.scores(
    inspection_id varchar DEFAULT NULL,
    dba_name varchar DEFAULT NULL,
    aka_name varchar DEFAULT NULL,
    license_ varchar DEFAULT NULL,
    facility_type varchar DEFAULT NULL,
    risk varchar DEFAULT NULL,
    address varchar DEFAULT NULL,
    city varchar DEFAULT NULL,
    state varchar DEFAULT NULL,
    inspection_date varchar DEFAULT NULL,
    inspection_type varchar DEFAULT NULL,
    results varchar DEFAULT NULL,
    latitude varchar DEFAULT NULL,
    longitude varchar DEFAULT NULL,
    location json DEFAULT NULL,
    violations varchar DEFAULT NULL,
    label integer DEFAULT NULL,
    predicted_labels integer DEFAULT NULL,
    predicted_score_0 float DEFAULT NULL,
    predicted_score_1 float DEFAULT NULL,
    model varchar DEFAULT NULL,
    created_at timestamp without time zone DEFAULT NULL
);