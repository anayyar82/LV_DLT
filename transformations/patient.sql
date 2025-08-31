-- ===========================================
-- 1) Main Silver Table with Auto Schema Evolution
-- ===========================================

CREATE OR REFRESH STREAMING TABLE silver_events_patient_data
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.feature.variantType-preview' = 'supported',
    'quality' = 'silver'
)
AS
SELECT
  ID,
  Shard,
  Private,
  Name,
  Address1,
  Address2,
  City,
  State,
  ZipCode,
  Country,
  PhoneNumber,
  BusinessID,
  Created,
  CreatedBy,
  Updated,
  UpdatedBy,
  V,
  P,
  D_struct.address1 AS D_address1,
  D_struct.address2 AS D_address2,
  D_struct.businessId AS D_businessId,
  D_struct.city AS D_city,
  D_struct.country AS D_country,
  D_struct.created AS D_created,
  D_struct.createdBy AS D_createdBy,
  D_struct.id AS D_id,
  D_struct.image AS D_image,
  D_struct.name AS D_name,
  D_struct.phoneNumber AS D_phoneNumber,
  D_struct.private AS D_private,
  D_struct.programs AS D_programs,
  D_struct.records AS D_records,
  D_struct.shard AS D_shard,
  D_struct.state AS D_state,
  D_struct.updated AS D_updated,
  D_struct.updatedBy AS D_updatedBy,
  D_struct.zipCode AS D_zipCode
FROM (
  SELECT
    *,
    from_json(
      D,
      'STRUCT<
        address1: STRING,
        address2: STRING,
        businessId: STRING,
        city: STRING,
        country: STRING,
        created: BIGINT,
        createdBy: STRING,
        id: STRING,
        image: STRING,
        name: STRING,
        phoneNumber: STRING,
        private: BOOLEAN,
        programs: STRING,
        records: ARRAY<STRUCT<
            disabled: BOOLEAN,
            id: STRING,
            name: STRING,
            optionData: STRUCT<
                checkBoxes: ARRAY<STRING>,
                data: STRING,
                fixedText: BOOLEAN
            >,
            priority: BIGINT,
            type: STRING
        >>,
        shard: BIGINT,
        state: STRING,
        updated: BIGINT,
        updatedBy: STRING,
        zipCode: STRING
      >'
    ) AS D_struct
  FROM STREAM bronze_events_patient_data
);

-- ===========================================
-- 2) Quarantine Table (Bad / Malformed Records)
-- ===========================================
CREATE OR REFRESH STREAMING TABLE quarantine_events_patient_data
COMMENT "Capture malformed or invalid JSON records"
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.feature.variantType-preview' = 'supported',
    'quality' = 'quarantine'
)
AS
SELECT
  ID,
  Shard,
  D AS raw_payload,
  current_timestamp() AS quarantined_at,
  "Malformed JSON in column D" AS error_message
FROM STREAM bronze_events_patient_data
WHERE D IS NOT NULL
  AND from_json(D, 'map<string,string>') IS NULL;

-- ===========================================
-- 3) Error Audit Table (Detailed Error Logs)
-- ===========================================
CREATE OR REFRESH STREAMING TABLE error_audit_events_patient_data
COMMENT "Detailed audit of ingestion errors"
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.feature.variantType-preview' = 'supported',
    'quality' = 'audit'
)
AS
SELECT
  ID,
  Shard,
  current_timestamp() AS error_detected_at,
  "json_parse_error" AS error_type,
  D AS raw_payload
FROM STREAM bronze_events_patient_data
WHERE D IS NOT NULL
  AND from_json(D, 'map<string,string>') IS NULL;

-- ===========================================
-- 4) New Fields Discovery (Schema Drift Detection)
-- ===========================================
CREATE OR REFRESH STREAMING TABLE new_fields_discovery_patient_data
COMMENT "Capture discovery of new JSON keys from column D"
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.feature.variantType-preview' = 'supported',
    'quality' = 'audit'
)
AS
SELECT
  current_timestamp() AS detected_at,
  ID,
  Shard,
  key   AS field_name,
  typeof(value) AS field_type,
  raw_payload
FROM (
  SELECT
    ID,
    Shard,
    D AS raw_payload,
    explode(map_entries(from_json(D, 'map<string,string>'))) AS kv
  FROM STREAM bronze_events_patient_data
) t
LATERAL VIEW INLINE (ARRAY(kv)) f AS key, value;

-- ===========================================
-- 5) Schema Audit (Track Actual Column Evolution)
-- ===========================================
CREATE OR REFRESH MATERIALIZED VIEW schema_audit_patient_data
COMMENT "Logs schema changes in silver_events_patient_data"
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'delta.feature.variantType-preview' = 'supported',
    'quality' = 'audit'
)
AS
SELECT
  current_timestamp() AS detected_at,
  column_name AS col_name,
  data_type,
  "silver_events_patient_data" AS target_table
FROM
  information_schema.columns
WHERE
  table_name = 'silver_events_patient_data';