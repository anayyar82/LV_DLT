-- ====================================================
-- 3️⃣ Silver table - parse VARIANT safely (triple-encoded JSON)
-- ====================================================

-- Create the Silver table
CREATE OR REFRESH STREAMING TABLE silver_events_patient_data
(
  ID STRING,
  Shard STRING,
  Private STRING,
  Name STRING,
  Address1 STRING,
  Address2 STRING,
  City STRING,
  State STRING,
  ZipCode STRING,
  Country STRING,
  PhoneNumber STRING,
  BusinessID STRING,
  Created STRING,
  CreatedBy STRING,
  Updated STRING,
  UpdatedBy STRING,

  -- Extracted from D_variant
  D_id STRING,
  D_name STRING,
  D_address1 STRING,
  D_address2 STRING,
  D_city STRING,
  D_state STRING,
  D_zipCode STRING,
  D_country STRING,
  D_phoneNumber STRING,
  D_businessId STRING,
  D_private BOOLEAN,
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

-- CDC Flow to populate the Silver table
CREATE FLOW silver_events_patient_data_cdc AS AUTO CDC INTO
  silver_events_patient_data
FROM (
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

    -- Parse triple-encoded JSON into VARIANT
    parse_json(
      regexp_replace(
        regexp_replace(
          substring(D, 2, length(D)-2),
          '""', '"'
        ),
        '\\\\"', '"'
      )
    ) AS D_variant,

    -- Flatten into structured columns
    D_variant:id::string           AS D_id,
    D_variant:name::string         AS D_name,
    D_variant:address1::string     AS D_address1,
    D_variant:address2::string     AS D_address2,
    D_variant:city::string         AS D_city,
    D_variant:state::string        AS D_state,
    D_variant:zipCode::string      AS D_zipCode,
    D_variant:country::string      AS D_country,
    D_variant:phoneNumber::string  AS D_phoneNumber,
    D_variant:businessId::string   AS D_businessId,
    D_variant:private::boolean     AS D_private,
    (D_variant:created::bigint)    AS D_created_epoch,
    (D_variant:updated::bigint)    AS D_updated_epoch,
    D_variant:createdBy::string    AS D_createdBy,
    D_variant:updatedBy::string    AS D_updatedBy,

    -- Convert epoch → timestamp
    to_timestamp(D_created_epoch) AS D_created,
    to_timestamp(D_updated_epoch) AS D_updated,

    current_timestamp() AS processedTime,

    -- CDC system columns
    _change_type, _commit_version, _commit_timestamp
  FROM (
    FROM STREAM(patient_cdf)
    SELECT *
  )
)
KEYS (ID, BusinessID)
APPLY AS DELETE WHEN _change_type = "delete"
APPLY AS TRUNCATE WHEN _change_type = "truncate"
SEQUENCE BY (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp, D_variant, D_created_epoch, D_updated_epoch)
STORED AS SCD TYPE 1;
