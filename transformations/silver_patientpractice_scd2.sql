-- ====================================================
-- SILVER PatientPractice SCD2 with schema auto-evolution
-- ====================================================

CREATE OR REFRESH STREAMING TABLE silver_patientpractice_scd2
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'silver'
) AS

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

    -- Explicit schema + evolution
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
        'schemaLocationKey', 'silver_patientpractice_D'   -- 👈 required for evolution
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

  -- Flatten known fields
  D_struct.*,

  -- Captures unknown/new fields until auto-promoted
  D_struct._rescued_data AS D_rescued_data,

  -- Technical audit
  current_timestamp()     AS processedTime,
  _change_type,
  _commit_version,
  _commit_timestamp
FROM parsed;
