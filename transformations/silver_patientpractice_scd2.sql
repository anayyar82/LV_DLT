-- ====================================================
-- SILVER PatientPractice SCD2 using AUTO CDC
-- ====================================================

-- Step 1: Stage table - parse JSON into struct with schema evolution
CREATE OR REFRESH STREAMING LIVE TABLE silver_patientpractice_scd2
AS
SELECT
    PatientID,
    PracticeID,
    Shard,
    Created,
    CreatedBy,
    Updated,
    UpdatedBy,

    -- Parse JSON column D
    FROM_JSON(
        D,
        'STRUCT<
            address1: STRING,
            address2: STRING,
            anonymous: BOOLEAN,
            businessId: STRING,
            city: STRING,
            country: STRING,
            created: BIGINT,
            createdBy: STRING,
            name: STRING,
            patientId: STRING,
            phoneNumber: STRING,
            practiceId: STRING,
            referrerId: STRING,
            shard: BIGINT,
            state: STRING,
            updated: BIGINT,
            updatedBy: STRING,
            zipCode: STRING
        >',
        MAP(
            'mode', 'PERMISSIVE',
            'rescuedDataColumn', '_rescued_data',
            'schemaEvolutionMode', 'addNewColumns',
            'schemaLocationKey', 'silver_patientpractice_D'
        )
    ) AS D_struct
FROM LIVE.bronze_patientpractice_cdf;

-- ====================================================
-- Silver PatientPractice SCD2 with AUTO CDC
-- ====================================================

-- Step 1: Create the CDC Flow
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

      -- SAFE JSON parsing with PERMISSIVE mode and schema evolution
      from_json(
        D,
        'STRUCT<
          address1: STRING,
          address2: STRING,
          anonymous: BOOLEAN,
          businessId: STRING,
          city: STRING,
          country: STRING,
          created: BIGINT,
          createdBy: STRING,
          name: STRING,
          patientId: STRING,
          phoneNumber: STRING,
          practiceId: STRING,
          referrerId: STRING,
          shard: BIGINT,
          state: STRING,
          updated: BIGINT,
          updatedBy: STRING,
          zipCode: STRING
        >',
        map(
          'mode', 'PERMISSIVE',
          'rescuedDataColumn', '_rescued_data',
          'schemaEvolutionMode', 'addNewColumns',
          'schemaLocationKey', 'silver_patientpractice_D'
        )
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

    -- Flatten JSON fields from D_struct
    D_struct.practiceId    AS D_practiceId,
    D_struct.patientId     AS D_patientId,
    D_struct.referrerId    AS D_referrerId,
    D_struct.name          AS D_name,
    D_struct.address1      AS D_address1,
    D_struct.address2      AS D_address2,
    D_struct.city          AS D_city,
    D_struct.state         AS D_state,
    D_struct.zipCode       AS D_zipCode,
    D_struct.country       AS D_country,
    D_struct.phoneNumber   AS D_phoneNumber,
    D_struct.businessId    AS D_businessId,
    D_struct.anonymous     AS D_anonymous,
    to_timestamp(D_struct.created) AS D_created,
    D_struct.createdBy     AS D_createdBy,
    to_timestamp(D_struct.updated) AS D_updated,
    D_struct.updatedBy     AS D_updatedBy,
    D_struct._rescued_data AS D_rescued_data,

    -- Optional: track processing time
    current_timestamp() AS processedTime,

    -- Keep CDC metadata
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

