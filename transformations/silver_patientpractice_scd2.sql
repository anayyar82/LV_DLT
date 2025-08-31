-- Create or refresh the streaming Silver table using schema inference/evolution for JSON
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
    from_json(D, NULL, map(
      'schemaLocationKey', 'silver_patientpractice_D',
      'schemaEvolutionMode', 'addNewColumns',
      'rescuedDataColumn', '_rescued_data'
    )) AS D_struct
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
  D_struct.practiceId        AS D_practiceId,
  D_struct.patientId         AS D_patientId,
  D_struct.referrerId        AS D_referrerId,
  D_struct.name              AS D_name,
  D_struct.address1          AS D_address1,
  D_struct.address2          AS D_address2,
  D_struct.city              AS D_city,
  D_struct.state             AS D_state,
  D_struct.zipCode           AS D_zipCode,
  D_struct.country           AS D_country,
  D_struct.phoneNumber       AS D_phoneNumber,
  D_struct.businessId        AS D_businessId,
  D_struct.anonymous         AS D_anonymous,
  to_timestamp(D_struct.created) AS D_created,
  D_struct.createdBy         AS D_createdBy,
  to_timestamp(D_struct.updated) AS D_updated,
  D_struct.updatedBy         AS D_updatedBy,
  D_struct._rescued_data     AS D_rescued_data,
  current_timestamp() AS processedTime,
  _change_type,
  _commit_version,
  _commit_timestamp
FROM parsed;


-- CDC SCD2 flow with proper keys, delete logic, sequencing, and SCD2 storage
CREATE FLOW silver_patientpractice_cdc_scd2 AS
  AUTO CDC INTO silver_patientpractice_scd2
FROM (
  SELECT * FROM silver_patientpractice_scd2
)
KEYS (PatientID, PracticeID)
APPLY AS DELETE WHEN
  _change_type = "delete"
SEQUENCE BY
  (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2;
