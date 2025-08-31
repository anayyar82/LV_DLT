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
      from_json(P, 'MAP<STRING, STRING>') as P,
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




PatientID:string
Shard:string
PracticeID:string
Created:string
CreatedBy:string
Updated:string
UpdatedBy:string
V:string
D:string
P:string
inputFilename:string
fullFilePath:string
fileMetadata:struct
bronze_prefix:string
ingestTime:timestamp
ingestDate:date
_change_type:string
_commit_version:long
_commit_timestamp:timestamp


{"address1":"010 Dicki Union","address2":"22025 Marlin Light","anonymous":false,"businessId":"idxdlfxc71","city":"North Nicolas","country":"US","created":1587214191,"createdBy":"17fdc071-8173-11ea-97b7-0242ac110008","name":"practicevjalt6k7","patientId":"17fdc071-8173-11ea-97b7-0242ac110008","phoneNumber":"01653453370","practiceId":"171ecdf0-8173-11ea-97b7-0242ac110008","referrerId":"13f70c75-8173-11ea-844f-0242ac11000b","shard":1836,"state":"Iowa","updated":1587214191,"updatedBy":"17fdc071-8173-11ea-97b7-0242ac110008","zipCode":"02665"}


{"171ecdf0-8173-11ea-97b7-0242ac110008":"w","17fdc071-8173-11ea-97b7-0242ac110008":"w"}

