-- ====================================================
-- Silver Device-Patient SCD2 with AUTO CDC and explicit schema
-- ====================================================

-- Step 1: Create or refresh the Silver table
CREATE OR REFRESH STREAMING TABLE silver_device_patient_scd2
(
  PracticeID STRING,
  Shard INT,
  PatientID STRING,
  Created TIMESTAMP,
  CreatedBy STRING,
  Updated TIMESTAMP,
  UpdatedBy STRING,
  V STRING,
  D STRING,  -- Original JSON string
  P MAP<STRING, STRING>,

  -- Flattened fields from D
  D_created TIMESTAMP,
  D_createdBy STRING,
  D_dateOfBirth TIMESTAMP,
  D_deviceTypeId INT,
  D_firstName STRING,
  D_lastName STRING,
  D_patientId STRING,
  D_practiceId STRING,
  D_shard INT,
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

-- Step 2: Create CDC flow
CREATE FLOW silver_device_patient_cdc_scd2 AS AUTO CDC INTO
  silver_device_patient_scd2
FROM (
  WITH parsed AS (
    SELECT
      PracticeID,
      Shard,
      PatientID,
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

      -- Parse JSON D using PERMISSIVE mode
      from_json(
        D,
        'STRUCT<
          created: BIGINT,
          createdBy: STRING,
          dateOfBirth: BIGINT,
          firstName: STRING,
          lastName: STRING,
          patientId: STRING,
          practiceId: STRING,
          shard: INT,
          updated: BIGINT,
          updatedBy: STRING
        >',
        map(
          'mode','PERMISSIVE',
          'rescuedDataColumn','_rescued_data',
          'schemaEvolutionMode','addNewColumns'
          )
      ) AS D_struct
    FROM STREAM(bronze_patientpractice_cdf)
  )
  SELECT
    PracticeID,
    Shard,
    PatientID,
    Created,
    CreatedBy,
    Updated,
    UpdatedBy,
    V,
    D,
    P,

    -- Flatten D_struct
    to_timestamp(D_struct.created) AS D_created,
    D_struct.createdBy AS D_createdBy,
    to_timestamp(D_struct.dateOfBirth) AS D_dateOfBirth,
    D_struct.firstName AS D_firstName,
    D_struct.lastName AS D_lastName,
    D_struct.patientId AS D_patientId,
    D_struct.practiceId AS D_practiceId,
    D_struct.shard AS D_shard,
    to_timestamp(D_struct.updated) AS D_updated,
    D_struct.updatedBy AS D_updatedBy,

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



Query [id = 8aeffb56-0e24-450c-980b-e7a20ab9a84d, runId = 99037b60-826d-4f26-9be7-0b96421aa95c] terminated with exception: [INCOMPATIBLE_COLUMN_TYPE] UNION can only be performed on tables with compatible column types. The 10th column of the second table is "STRING" type which is not compatible with "MAP<STRING, STRING>" at the same column of the first table.. SQLSTATE: 42825;
'Union false, false
:- Project [PracticeID#425674, cast(Shard#425675 as string) AS Shard#426242, PatientID#425676, cast(Created#425677 as string) AS Created#426243, CreatedBy#425678, cast(Updated#425679 as string) AS Updated#426244, UpdatedBy#425680, V#425681, D#425682, P#425683, D_created#425684, D_createdBy#425685, D_dateOfBirth#425686, D_firstName#425688, D_lastName#425689, D_patientId#425690, D_practiceId#425691, D_shard#425692, D_updated#425693, D_updatedBy#425694, processedTime#425695, __recordStartAt#425890, __START_AT#425697, __END_AT#425698]
:  +- Project [PracticeID#425674, Shard#425675, PatientID#425676, Created#425677, CreatedBy#425678, Updated#425679, UpdatedBy#425680, V#425681, D#425682, P#425683, D_created#425684, D_createdBy#425685, D_dateOfBirth#425686, D_firstName#425688, D_lastName#425689, D_patientId#425690, D_practiceId#425691, D_shard#425692, D_updated#425693, D_updatedBy#425694, processedTime#425695, __recordStartAt#425890, __START_AT#425697, __END_AT#425698]
:     +- Join LeftSemi, ((PatientID#425676 <=> PatientID#426042) AND ((coalesce(__recordStartAt#425890, __END_AT#425698) >= __sequencing#426075) OR isnull(coalesce(__recordStartAt#425890, __END_AT#425698))))
