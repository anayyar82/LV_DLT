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
    -- from_json(D, NULL, map(
    --   'schemaLocationKey', 'silver_patientpractice_D',
    --   'schemaEvolutionMode', 'addNewColumns',
    --   'rescuedDataColumn', '_rescued_data'
    -- )) AS D_struct
    D,
  --from_json(D, NULL, map('schemaLocationKey', 'keyX')) parsedX
    -- If a new column appears, the pipeline will automatically add it to the schema:
    parse_json(d) as variant_col
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
  D,
  variant_col:address1::string as D_address1
 
FROM parsed;


-- -- CDC SCD2 flow with proper keys, delete logic, sequencing, and SCD2 storage
-- CREATE FLOW silver_patientpractice_cdc_scd2 AS
--   AUTO CDC INTO silver_patientpractice_scd2
-- FROM (
--   SELECT * FROM silver_patientpractice_scd2
-- )
-- KEYS (PatientID, PracticeID)
-- APPLY AS DELETE WHEN
--   _change_type = "delete"
-- SEQUENCE BY
--   (_commit_version, _commit_timestamp)
-- COLUMNS * EXCEPT
--   (_change_type, _commit_version, _commit_timestamp)
-- STORED AS SCD TYPE 2;

-- {"address1":"010 Dicki Union","address2":"22025 Marlin Light","anonymous":false,"businessId":"idxdlfxc71","city":"North Nicolas","country":"US","created":1587214191,"createdBy":"17fdc071-8173-11ea-97b7-0242ac110008","name":"practicevjalt6k7","patientId":"17fdc071-8173-11ea-97b7-0242ac110008","phoneNumber":"01653453370","practiceId":"171ecdf0-8173-11ea-97b7-0242ac110008","referrerId":"13f70c75-8173-11ea-844f-0242ac11000b","shard":1836,"state":"Iowa","updated":1587214191,"updatedBy":"17fdc071-8173-11ea-97b7-0242ac110008","zipCode":"02665"}
