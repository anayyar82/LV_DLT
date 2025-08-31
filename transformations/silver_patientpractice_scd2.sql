CREATE OR REFRESH STREAMING TABLE silver_patientpractice_scd2
(
  PatientID STRING,
  PracticeID STRING,
  Shard STRING,
  Created STRING,
  CreatedBy STRING,
  Updated STRING,
  UpdatedBy STRING,

  -- Flattened JSON fields directly
  D_practiceId STRING,
  D_patientId STRING,
  D_referrerId STRING,
  D_name STRING,
  D_address1 STRING,
  D_address2 STRING,
  D_city STRING,
  D_state STRING,
  D_zipCode STRING,
  D_country STRING,
  D_phoneNumber STRING,
  D_businessId STRING,
  D_anonymous BOOLEAN,
  D_created TIMESTAMP,
  D_createdBy STRING,
  D_updated TIMESTAMP,
  D_updatedBy STRING,

  processedTime TIMESTAMP
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'silver'
);

CREATE FLOW silver_patientpractice_cdc_scd2 AS AUTO CDC INTO
  silver_patientpractice_scd2
FROM (
  SELECT
    PatientID,
    PracticeID,
    Shard,
    Created,
    CreatedBy,
    Updated,
    UpdatedBy,
    parse_json(D):practiceId::string   AS D_practiceId,
    parse_json(D):patientId::string    AS D_patientId,
    parse_json(D):referrerId::string   AS D_referrerId,
    parse_json(D):name::string         AS D_name,
    parse_json(D):address1::string     AS D_address1,
    parse_json(D):address2::string     AS D_address2,
    parse_json(D):city::string         AS D_city,
    parse_json(D):state::string        AS D_state,
    parse_json(D):zipCode::string      AS D_zipCode,
    parse_json(D):country::string      AS D_country,
    parse_json(D):phoneNumber::string  AS D_phoneNumber,
    parse_json(D):businessId::string   AS D_businessId,
    parse_json(D):anonymous::boolean   AS D_anonymous,
    to_timestamp(parse_json(D):created::bigint) AS D_created,
    parse_json(D):createdBy::string    AS D_createdBy,
    to_timestamp(parse_json(D):updated::bigint) AS D_updated,
    parse_json(D):updatedBy::string    AS D_updatedBy,
    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM STREAM(bronze_patientpractice_cdf)
)
KEYS (PatientID, PracticeID)
APPLY AS DELETE WHEN _change_type = "delete"
SEQUENCE BY (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2;
