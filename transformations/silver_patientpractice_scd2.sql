-- ====================================================
-- Silver PatientPractice SCD2 with structured JSON parsing
-- ====================================================

-- 1️⃣ Create the streaming table
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

-- 2️⃣ Define the schema for D column
CREATE OR REFRESH STREAMING TABLE bronze_patientpractice_cdf_struct AS
SELECT
  *,
  from_json(
    regexp_replace(
      regexp_replace(
        substring(D, 2, length(D)-2),
        '""', '"'
      ),
      '\\\\"', '"'
    ),
    'STRUCT<
      practiceId: STRING,
      patientId: STRING,
      referrerId: STRING,
      name: STRING,
      address1: STRING,
      address2: STRING,
      city: STRING,
      state: STRING,
      zipCode: STRING,
      country: STRING,
      phoneNumber: STRING,
      businessId: STRING,
      anonymous: BOOLEAN,
      created: BIGINT,
      createdBy: STRING,
      updated: BIGINT,
      updatedBy: STRING
    >',
    map('mode','PERMISSIVE')
  ) AS D_struct
FROM STREAM(bronze_patientpractice_cdf);

-- 3️⃣ Create the CDC flow
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

    -- Flatten JSON STRUCT fields
    D_struct:practiceId::STRING   AS D_practiceId,
    D_struct:patientId::STRING    AS D_patientId,
    D_struct:referrerId::STRING   AS D_referrerId,
    D_struct:name::STRING         AS D_name,
    D_struct:address1::STRING     AS D_address1,
    D_struct:address2::STRING     AS D_address2,
    D_struct:city::STRING         AS D_city,
    D_struct:state::STRING        AS D_state,
    D_struct:zipCode::STRING      AS D_zipCode,
    D_struct:country::STRING      AS D_country,
    D_struct:phoneNumber::STRING  AS D_phoneNumber,
    D_struct:businessId::STRING   AS D_businessId,
    D_struct:anonymous::BOOLEAN   AS D_anonymous,
    to_timestamp(D_struct:created::BIGINT)   AS D_created,
    D_struct:createdBy::STRING    AS D_createdBy,
    to_timestamp(D_struct:updated::BIGINT)   AS D_updated,
    D_struct:updatedBy::STRING    AS D_updatedBy,

    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM bronze_patientpractice_cdf_struct
)
KEYS (PatientID, PracticeID)
APPLY AS DELETE WHEN _change_type = "delete"
SEQUENCE BY (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2;
