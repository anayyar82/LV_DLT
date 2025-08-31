-- ====================================================
-- Silver PatientPractice SCD2 using semi_structured_extract_json_multi
-- ====================================================

-- Create or refresh the Silver table
CREATE OR REFRESH STREAMING TABLE silver_patientpractice_scd2
(
  PatientID STRING,
  PracticeID STRING,
  Shard STRING,
  Created STRING,
  CreatedBy STRING,
  Updated STRING,
  UpdatedBy STRING,

  -- Flattened fields from D JSON
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

-- Create the CDC Flow
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
    
    -- Extract JSON fields from D string
    semi_structured_extract_json_multi(D, '$.practiceId')     AS D_practiceId,
    semi_structured_extract_json_multi(D, '$.patientId')      AS D_patientId,
    semi_structured_extract_json_multi(D, '$.referrerId')     AS D_referrerId,
    semi_structured_extract_json_multi(D, '$.name')           AS D_name,
    semi_structured_extract_json_multi(D, '$.address1')       AS D_address1,
    semi_structured_extract_json_multi(D, '$.address2')       AS D_address2,
    semi_structured_extract_json_multi(D, '$.city')           AS D_city,
    semi_structured_extract_json_multi(D, '$.state')          AS D_state,
    semi_structured_extract_json_multi(D, '$.zipCode')        AS D_zipCode,
    semi_structured_extract_json_multi(D, '$.country')        AS D_country,
    semi_structured_extract_json_multi(D, '$.phoneNumber')    AS D_phoneNumber,
    semi_structured_extract_json_multi(D, '$.businessId')     AS D_businessId,
    cast(semi_structured_extract_json_multi(D, '$.anonymous') AS BOOLEAN) AS D_anonymous,
    to_timestamp(cast(semi_structured_extract_json_multi(D, '$.created') AS BIGINT)) AS D_created,
    semi_structured_extract_json_multi(D, '$.createdBy')      AS D_createdBy,
    to_timestamp(cast(semi_structured_extract_json_multi(D, '$.updated') AS BIGINT)) AS D_updated,
    semi_structured_extract_json_multi(D, '$.updatedBy')      AS D_updatedBy,

    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM STREAM(bronze_patientpractice_cdf)
)
KEYS (PatientID, PracticeID)
APPLY AS DELETE WHEN
  _change_type = "delete"
SEQUENCE BY
  (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp)
STORED AS
  SCD TYPE 2;
