-- ====================================================
-- Silver Device-Patient SCD2 with AUTO CDC (UTF-16LE JSON handling)
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
  D STRING,  -- Original JSON string
  P MAP<STRING, STRING>,  -- permissions map

  -- Flattened fields from D
  D_address1 STRING,
  D_address2 STRING,
  D_anonymous BOOLEAN,
  D_businessId STRING,
  D_city STRING,
  D_country STRING,
  D_created TIMESTAMP,
  D_createdBy STRING,
  D_name STRING,
  D_patientId STRING,
  D_phoneNumber STRING,
  D_practiceId STRING,
  D_referrerId STRING,
  D_shard INT,
  D_state STRING,
  D_updated TIMESTAMP,
  D_updatedBy STRING,
  D_zipCode STRING,

  processedTime TIMESTAMP
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'quality' = 'silver'
);

-- Step 2: Create CDC flow
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
      -- Convert P JSON string to MAP<STRING, STRING>
      --P,
      ingestTime,
      _change_type,
      _commit_version,
      _commit_timestamp,

      -- Parse D JSON safely (UTF-16LE → UTF-8, unescape)
      -- from_json(D, 
      --    'address1 STRING,
      --     address2 STRING,
      --     anonymous BOOLEAN,
      --     businessId STRING,
      --     city STRING,
      --     country STRING,
      --     created BIGINT,
      --     createdBy STRING,
      --     name STRING,
      --     patientId STRING,
      --     phoneNumber STRING,
      --     practiceId STRING,
      --     referrerId STRING,
      --     shard INT,
      --     state STRING,
      --     updated BIGINT,
      --     updatedBy STRING,
      --     zipCode STRING' ) AS D_struct

      from_json(D, 'address1 STRING,
          address2 STRING,
          anonymous BOOLEAN,
          businessId STRING,
          city STRING,
          country STRING,
          created BIGINT,
          createdBy STRING,
          name STRING,
          patientId STRING,
          phoneNumber STRING,
          practiceId STRING,
          referrerId STRING,
          shard INT,
          state STRING,
          updated BIGINT,
          updatedBy STRING,
          zipCode STRING') D_struct

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
    -- P,
    -- Flatten D_struct
    D_struct:address1 AS D_address1,
    -- D_struct.address2 AS D_address2,
    -- D_struct.anonymous AS D_anonymous,
    -- D_struct.businessId AS D_businessId,
    -- D_struct.city AS D_city,
    -- D_struct.country AS D_country,
    -- to_timestamp(D_struct.created) AS D_created,
    -- D_struct.createdBy AS D_createdBy,
    -- D_struct.name AS D_name,
    -- D_struct.patientId AS D_patientId,
    -- D_struct.phoneNumber AS D_phoneNumber,
    -- D_struct.practiceId AS D_practiceId,
    -- D_struct.referrerId AS D_referrerId,
    -- D_struct.shard AS D_shard,
    -- D_struct.state AS D_state,
    -- to_timestamp(D_struct.updated) AS D_updated,
    -- D_struct.updatedBy AS D_updatedBy,
    -- D_struct.zipCode AS D_zipCode,

    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM parsed
)
KEYS (PatientID)
APPLY AS DELETE WHEN
  _change_type = "delete"
SEQUENCE BY
  (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp)
STORED AS
  SCD TYPE 2;


-- map(
--           'mode','PERMISSIVE',
--           'rescuedDataColumn','_rescued_data',
--           'schemaEvolutionMode','addNewColumns'
--         )


-- Cannot resolve "semi_structured_extract_json_multi(D_struct, $.address1)" due to data type mismatch: The first parameter requires the "STRING" type, however "D_struct" has the type "STRUCT<address1: STRING, address2: STRING, anonymous: BOOLEAN, businessId: STRING, city: STRING, country: STRING, created: BIGINT, createdBy: STRING, name: STRING, patientId: STRING, phoneNumber: STRING, practiceId: STRING, referrerId: STRING, shard: INT, state: STRING, updated: BIGINT, updatedBy: STRING, zipCode: STRING>".
