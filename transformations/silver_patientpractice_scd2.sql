-- ====================================================
-- Silver PatientPractice SCD2 with parse_json(value_str)
-- ====================================================
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

  -- Source metadata
  source STRUCT<
    topic: STRING,
    partition: INT,
    offset: BIGINT,
    timestamp: TIMESTAMP_LTZ,
    timestampType: INT,
    ingestTime: TIMESTAMP_LTZ
  >,

  processedTime TIMESTAMP
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'silver'
);

CREATE FLOW silver_patientpractice_cdc_scd2 AS AUTO CDC INTO
  silver_patientpractice_scd2
FROM (
  FROM STREAM(bronze_patientpractice_cdf) |>
  SELECT *,
    -- Parse the JSON string safely
    parse_json(value_str) AS variant_col
  |>
  SELECT
    PatientID,
    PracticeID,
    Shard,
    Created,
    CreatedBy,
    Updated,
    UpdatedBy,
    -- Flatten JSON fields
    variant_col:practiceId::string   AS D_practiceId,
    variant_col:patientId::string    AS D_patientId,
    variant_col:referrerId::string   AS D_referrerId,
    variant_col:name::string         AS D_name,
    variant_col:address1::string     AS D_address1,
    variant_col:address2::string     AS D_address2,
    variant_col:city::string         AS D_city,
    variant_col:state::string        AS D_state,
    variant_col:zipCode::string      AS D_zipCode,
    variant_col:country::string      AS D_country,
    variant_col:phoneNumber::string  AS D_phoneNumber,
    variant_col:businessId::string   AS D_businessId,
    variant_col:anonymous::boolean   AS D_anonymous,
    to_timestamp(variant_col:created::bigint)   AS D_created,
    variant_col:createdBy::string    AS D_createdBy,
    to_timestamp(variant_col:updated::bigint)   AS D_updated,
    variant_col:updatedBy::string    AS D_updatedBy,
    -- Build source metadata
    named_struct(
      'topic', topic,
      'partition', partition,
      'offset', offset,
      'timestamp', timestamp,
      'timestampType', timestampType,
      'ingestTime', ingestTime
    ) AS source,
    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
)
KEYS (PatientID, PracticeID)
APPLY AS DELETE WHEN
  _change_type = "delete"
SEQUENCE BY
  (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT
  (_change_type, _commit_version, _commit_timestamp, topic, partition, offset, timestamp, timestampType, ingestTime)
STORED AS
  SCD TYPE 2;
