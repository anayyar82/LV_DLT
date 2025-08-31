-- ====================================================
-- Silver Device-Patient SCD2 with AUTO CDC and explicit schema
-- ====================================================

-- Step 1: Create or refresh the Silver table
CREATE OR REFRESH STREAMING TABLE silver_device_patient_scd2
(
  PracticeID STRING,
  Shard INT,
  DeviceTypeID INT,
  SerialNumber STRING,
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
  D_serialNumber STRING,
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
      DeviceTypeID,
      SerialNumber,
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
          deviceTypeId: INT,
          firstName: STRING,
          lastName: STRING,
          patientId: STRING,
          practiceId: STRING,
          serialNumber: STRING,
          shard: INT,
          updated: BIGINT,
          updatedBy: STRING
        >',
        map(
          'mode','PERMISSIVE',
          'rescuedDataColumn','_rescued_data',
          'schemaEvolutionMode','addNewColumns',
          'schemaLocationKey','silver_device_patient_D'
        )
      ) AS D_struct
    FROM STREAM(bronze_device_patient_cdf)
  )
  SELECT
    PracticeID,
    Shard,
    DeviceTypeID,
    SerialNumber,
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
    D_struct.deviceTypeId AS D_deviceTypeId,
    D_struct.firstName AS D_firstName,
    D_struct.lastName AS D_lastName,
    D_struct.patientId AS D_patientId,
    D_struct.practiceId AS D_practiceId,
    D_struct.serialNumber AS D_serialNumber,
    D_struct.shard AS D_shard,
    to_timestamp(D_struct.updated) AS D_updated,
    D_struct.updatedBy AS D_updatedBy,

    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM parsed
)
KEYS (PatientID, SerialNumber)
APPLY AS DELETE WHEN
  _change_type = "delete"
SEQUENCE BY
  (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp)
STORED AS
  SCD TYPE 2;
