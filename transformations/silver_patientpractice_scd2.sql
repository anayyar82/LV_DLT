-- ====================================================
-- Silver PatientPractice SCD2 with safe parse_json(D string)
-- ====================================================

-- Create the Silver table
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

-- Create the CDC Flow
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

      -- Safely parse JSON string into STRUCT
      from_json(D, 
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

    -- Flatten STRUCT fields
    D_struct:practiceId        AS D_practiceId,
    D_struct:patientId         AS D_patientId,
    D_struct:referrerId        AS D_referrerId,
    D_struct:name              AS D_name,
    D_struct:address1          AS D_address1,
    D_struct:address2          AS D_address2,
    D_struct:city              AS D_city,
    D_struct:state             AS D_state,
    D_struct:zipCode           AS D_zipCode,
    D_struct:country           AS D_country,
    D_struct:phoneNumber       AS D_phoneNumber,
    D_struct:businessId        AS D_businessId,
    D_struct:anonymous         AS D_anonymous,
    to_timestamp(D_struct:created) AS D_created,
    D_struct:createdBy         AS D_createdBy,
    to_timestamp(D_struct:updated) AS D_updated,
    D_struct:updatedBy         AS D_updatedBy,

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
