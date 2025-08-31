-- ====================================================
-- 1️⃣ Silver PatientPractice SCD2 with safe JSON parsing
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

  -- Extracted from D JSON
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
  WITH parsed AS (
    SELECT
      PatientID,
      Shard,
      PracticeID,
      Created,
      CreatedBy,
      Updated,
      UpdatedBy,
      -- SAFE triple-encoded JSON parsing
      parse_json(
        regexp_replace(
          regexp_replace(
            substring(D, 2, length(D)-2),
            '""', '"'
          ),
          '\\\\"', '"'
        )
      ) AS D_variant,
      P,
      V,
      ingestTime,
      _change_type,
      _commit_version,
      _commit_timestamp
    FROM STREAM(bronze_patientpractice_cdf)
  )
  SELECT
    PatientID,
    Shard,
    PracticeID,
    Created,
    CreatedBy,
    Updated,
    UpdatedBy,
    D_variant,
    -- -- Flatten JSON fields
    -- D_variant:practiceId::string   AS D_practiceId,
    -- D_variant:patientId::string    AS D_patientId,
    -- D_variant:referrerId::string   AS D_referrerId,
    -- D_variant:name::string         AS D_name,
    -- D_variant:address1::string     AS D_address1,
    -- D_variant:address2::string     AS D_address2,
    -- D_variant:city::string         AS D_city,
    -- D_variant:state::string        AS D_state,
    -- D_variant:zipCode::string      AS D_zipCode,
    -- D_variant:country::string      AS D_country,
    -- D_variant:phoneNumber::string  AS D_phoneNumber,
    -- D_variant:businessId::string   AS D_businessId,
    -- D_variant:anonymous::boolean   AS D_anonymous,
    -- to_timestamp(D_variant:created::bigint)   AS D_created,
    -- D_variant:createdBy::string    AS D_createdBy,
    -- to_timestamp(D_variant:updated::bigint)   AS D_updated,
    -- D_variant:updatedBy::string    AS D_updatedBy,
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
