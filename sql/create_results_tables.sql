set time zone  'America/Mexico_City';

--schema results
DROP SCHEMA IF EXISTS results CASCADE;
CREATE SCHEMA results;

DROP TABLE IF EXISTS semantic.aequitas;

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
