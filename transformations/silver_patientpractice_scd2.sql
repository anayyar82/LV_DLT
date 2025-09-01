-- ====================================================
-- Silver Device-Patient SCD2 with AUTO CDC (UTF-16LE JSON handling) AND UTF 8
-- ====================================================

-- Step 1: Create or refresh the Silver table
CREATE OR REFRESH STREAMING TABLE silver_device_patient_scd2
(
  PatientID STRING,
  Shard STRING,
  PracticeID STRING,
  Created STRING,
  CreatedBy STRING,
  Updated STRING,
  UpdatedBy STRING,
  V STRING,
  D STRING,               -- Original JSON string
  P STRING,  -- permissions map

    -- Extracted from D_variant
  D_id STRING,
  D_name STRING,
  D_address1 STRING,
  D_address2 STRING,
  D_city STRING,
  D_state STRING,
  D_zipCode STRING,
  D_country STRING,
  D_phoneNumber STRING,
  D_businessId STRING,
  D_private BOOLEAN,
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
  'quality' = 'silver'
);

-- Step 2: Create the CDC flow
CREATE FLOW silver_device_patient_cdc_scd2 AS AUTO CDC INTO
  silver_device_patient_scd2
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
      V,
      D,
      P,
      ingestTime,
      _change_type,
      _commit_version,
      _commit_timestamp,

      -- Parse triple-encoded JSON into VARIANT
      parse_json(
        regexp_replace(
          regexp_replace(
            substring(D, 2, length(D)-2),
            '""', '"'
          ),
          '\\\\"', '"'
        )
      ) AS D_variant

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
    V,
    D,
    P,

    -- Flatten into structured columns
    D_variant:id::string           AS D_id,
    D_variant:name::string         AS D_name,
    D_variant:address1::string     AS D_address1,
    D_variant:address2::string     AS D_address2,
    D_variant:city::string         AS D_city,
    D_variant:state::string        AS D_state,
    D_variant:zipCode::string      AS D_zipCode,
    D_variant:country::string      AS D_country,
    D_variant:phoneNumber::string  AS D_phoneNumber,
    D_variant:businessId::string   AS D_businessId,
    D_variant:private::boolean     AS D_private,
    D_variant:createdBy::string    AS D_createdBy,
    D_variant:updatedBy::string    AS D_updatedBy,
    to_timestamp(D_variant:created::bigint) AS D_created,
    to_timestamp(D_variant:updated::bigint) AS D_updated,

    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM parsed
)
KEYS (PatientID)
APPLY AS DELETE WHEN _change_type = "delete"
SEQUENCE BY (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2;
