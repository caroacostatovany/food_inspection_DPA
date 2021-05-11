set time zone  'America/Mexico_City';

--schema semantic
DROP SCHEMA IF EXISTS semantic CASCADE;
CREATE SCHEMA semantic;

DROP TABLE IF EXISTS semantic.aequitas;

CREATE TABLE semantic.aequitas(
    facility_type varchar DEFAULT NULL,
    zip varchar DEFAULT NULL,
    inspection_type varchar DEFAULT NULL,
    label_value integer DEFAULT NULL,
    score integer DEFAULT NULL
);
