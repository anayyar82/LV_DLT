-- ====================================================
-- Silver PatientPractice SCD2 with safe PERMISSIVE JSON parsing
-- ====================================================
CREATE OR REFRESH STREAMING TABLE silver_patientpractice_scd2
(
  PatientID STRING,
  PracticeID STRING,
  Shard STRING,
  Created STRING,
  CreatedBy STRING,
  Updated STRING,
  UpdatedBy STRING,

  -- Flattened from D JSON
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

-- ====================================================
-- CDC Flow
-- ====================================================
CREATE FLOW silver_patientpractice_cdc_scd2 AS AUTO CDC INTO
  silver_patientpractice_scd2
FROM (
  WITH parsed AS (
    SELECT
      PatientID,
      PracticeID,
      Shard,
      Created,
      CreatedBy,
      Updated,
      UpdatedBy,
      _change_type,
      _commit_version,
      _commit_timestamp,

      -- SAFE triple-encoded JSON parsing with PERMISSIVE mode
      from_json(
        D,
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
      ) AS variant_col
    FROM STREAM(bronze_patientpractice_cdf)
  )
  SELECT
    PatientID,
    PracticeID,
    Shard,
    Created,
    CreatedBy,
    Updated,
    UpdatedBy,
    -- Flatten JSON fields
    variant_col:practiceId::string   AS D_practiceId,
    variant_col:patientId::string    AS D_patientId,
    variant_col:referrerId::string   AS D_referrerId,
    variant_col:name::string         AS D_name,
    variant_col:address1::string     AS D_address1,
    variant_col:address2::string     AS D_address2,
    variant_col:city::string         AS D_city,
    variant_col:state::string        AS D_state,
    variant_col:zipCode::string      AS D_zipCode,
    variant_col:country::string      AS D_country,
    variant_col:phoneNumber::string  AS D_phoneNumber,
    variant_col:businessId::string   AS D_businessId,
    variant_col:anonymous::boolean   AS D_anonymous,
    to_timestamp(variant_col:created::bigint)   AS D_created,
    variant_col:createdBy::string    AS D_createdBy,
    to_timestamp(variant_col:updated::bigint)   AS D_updated,
    variant_col:updatedBy::string    AS D_updatedBy,
    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM parsed
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


  Cannot resolve "semi_structured_extract_json_multi(variant_col, $.practiceId)" due to data type mismatch: The first parameter requires the "STRING" type, however "variant_col" has the type "STRUCT<practiceId: STRING, patientId: STRING, referrerId: STRING, name: STRING, address1: STRING, address2: STRING, city: STRING, state: STRING, zipCode: STRING, country: STRING, phoneNumber: STRING, businessId: STRING, anonymous: BOOLEAN, created: BIGINT, createdBy: STRING, updated: BIGINT, updatedBy: STRING>".

