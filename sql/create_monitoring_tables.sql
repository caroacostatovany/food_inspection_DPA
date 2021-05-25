set time zone  'America/Mexico_City';

--schema monitoring
DROP SCHEMA IF EXISTS monitoring CASCADE;
CREATE SCHEMA monitoring;

--scores
DROP TABLE IF EXISTS monitoring.scores;

CREATE TABLE monitoring.scores(
    inspection_id varchar DEFAULT NULL,
    label integer DEFAULT NULL,
    predicted_labels integer DEFAULT NULL,
    predicted_score_0 float DEFAULT NULL,
    predicted_score_1 float DEFAULT NULL,
    model varchar DEFAULT NULL,
    created_at date DEFAULT NULL
);