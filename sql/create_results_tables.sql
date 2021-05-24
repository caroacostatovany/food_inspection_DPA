set time zone  'America/Mexico_City';

--schema results
DROP SCHEMA IF EXISTS results CASCADE;
CREATE SCHEMA results;

DROP TABLE IF EXISTS results.sesgo;

CREATE TABLE results.sesgo(
    attribute_name varchar DEFAULT NULL,
    attribute_value varchar DEFAULT NULL,
    ppr_disparity float DEFAULT NULL,
    pprev_disparity float DEFAULT NULL,
    precision_disparity float DEFAULT NULL,
    fdr_disparity float DEFAULT NULL,
    for_disparity float DEFAULT NULL,
    fpr_disparity float DEFAULT NULL,
    fnr_disparity float DEFAULT NULL,
    tpr_disparity float DEFAULT NULL,
    tnr_disparity float DEFAULT NULL,
    npv_disparity float DEFAULT NULL
);

DROP TABLE IF EXISTS results.validation;

CREATE TABLE results.validation(
    inspection_id varchar DEFAULT NULL,
    label integer DEFAULT NULL,
    predicted_labels integer DEFAULT NULL,
    predicted_score_0 float DEFAULT NULL,
    predicted_score_1 float DEFAULT NULL,
    model varchar DEFAULT NULL,
    created_at date DEFAULT NULL
);
