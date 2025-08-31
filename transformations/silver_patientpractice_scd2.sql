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

      -- Parse JSON string safely into STRUCT
      from_json(
        regexp_replace(
          regexp_replace(
            substring(D, 2, length(D)-2),
            '""', '"'
          ),
          '\\\\"', '"'
        ),
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
    D_struct:practiceId::STRING   AS D_practiceId,
    D_struct:patientId::STRING    AS D_patientId,
    D_struct:referrerId::STRING   AS D_referrerId,
    D_struct:name::STRING         AS D_name,
    D_struct:address1::STRING     AS D_address1,
    D_struct:address2::STRING     AS D_address2,
    D_struct:city::STRING         AS D_city,
    D_struct:state::STRING        AS D_state,
    D_struct:zipCode::STRING      AS D_zipCode,
    D_struct:country::STRING      AS D_country,
    D_struct:phoneNumber::STRING  AS D_phoneNumber,
    D_struct:businessId::STRING   AS D_businessId,
    D_struct:anonymous::BOOLEAN   AS D_anonymous,
    to_timestamp(D_struct:created::BIGINT) AS D_created,
    D_struct:createdBy::STRING    AS D_createdBy,
    to_timestamp(D_struct:updated::BIGINT) AS D_updated,
    D_struct:updatedBy::STRING    AS D_updatedBy,
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




PatientID
string
Shard
string
PracticeID
string
Created
string
CreatedBy
string
Updated
string
UpdatedBy
string
V
string
D
string
P
string
inputFilename
string
fullFilePath
string
fileMetadata: {"file_path": "string", "file_name": "string", "file_size": "bigint", "file_block_start": "bigint", "file_block_length": "bigint", "file_modification_time": "timestamp"}
struct
bronze_prefix
string
ingestTime
timestamp
ingestDate
date


{"address1":"010 Dicki Union","address2":"22025 Marlin Light","anonymous":false,"businessId":"idxdlfxc71","city":"North Nicolas","country":"US","created":1587214191,"createdBy":"17fdc071-8173-11ea-97b7-0242ac110008","name":"practicevjalt6k7","patientId":"17fdc071-8173-11ea-97b7-0242ac110008","phoneNumber":"01653453370","practiceId":"171ecdf0-8173-11ea-97b7-0242ac110008","referrerId":"13f70c75-8173-11ea-844f-0242ac11000b","shard":1836,"state":"Iowa","updated":1587214191,"updatedBy":"17fdc071-8173-11ea-97b7-0242ac110008","zipCode":"02665"}