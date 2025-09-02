-- ====================================================
-- Silver table with structured records under D column
-- ====================================================
CREATE OR REFRESH STREAMING TABLE silver_events_patient_data_scd2
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

  -- Flattened known JSON fields
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

  -- Catch-all for unexpected fields
  D_extra MAP<STRING, STRING>,

  -- Structured records array
  D_records ARRAY<STRUCT<
      record_id: STRING,
      record_name: STRING,
      optionData: STRUCT<
          checkBoxes: ARRAY<STRING>,
          data: STRING,
          fixedText: BOOLEAN
      >,
      priority: INT,
      type: STRING
  >>,

  processedTime TIMESTAMP
)
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'silver'
);

-- ====================================================
-- Silver Flow: parse JSON, handle new keys, extract structured records
-- ====================================================
CREATE FLOW silver_events_patient_data_cdc_scd2 AS AUTO CDC INTO
  silver_events_patient_data_scd2
FROM (
  WITH cleaned AS (
    SELECT
      *,
      CASE
        WHEN D LIKE '"{%"}"' THEN
          replace(
            regexp_replace(
              regexp_replace(substring(D, 2, length(D)-2), '""', '"'),
              '\\\\"', '"'
            ),
            '\\u0000', ''
          )
        ELSE
          replace(D, '\\u0000', '')
      END AS D_clean
    FROM STREAM(patient_cdf)
  ),
  parsed AS (
    SELECT
      *,
      from_json(D_clean, 'MAP<STRING,STRING>') AS D_map
    FROM cleaned
  )
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

    -- Flatten known fields from D_map
    D_map['id'] AS D_id,
    D_map['name'] AS D_name,
    D_map['address1'] AS D_address1,
    D_map['address2'] AS D_address2,
    D_map['city'] AS D_city,
    D_map['state'] AS D_state,
    D_map['zipCode'] AS D_zipCode,
    D_map['country'] AS D_country,
    D_map['phoneNumber'] AS D_phoneNumber,
    D_map['businessId'] AS D_businessId,
    CAST(D_map['private'] AS BOOLEAN) AS D_private,
    to_timestamp(D_map['created']) AS D_created,
    D_map['createdBy'] AS D_createdBy,
    to_timestamp(D_map['updated']) AS D_updated,
    D_map['updatedBy'] AS D_updatedBy,

    -- Keep unknown fields in D_extra
    map_filter(D_map, (k, v) ->
      k NOT IN ('id','name','address1','address2','city','state','zipCode','country',
                'phoneNumber','businessId','private','created','createdBy','updated','updatedBy','records')
    ) AS D_extra,

    -- Parse records into structured array
    CASE
      WHEN D_map['records'] IS NOT NULL THEN
        from_json(D_map['records'], 'ARRAY<STRUCT<
            record_id: STRING,
            record_name: STRING,
            optionData: STRUCT<
                checkBoxes: ARRAY<STRING>,
                data: STRING,
                fixedText: BOOLEAN
            >,
            priority: INT,
            type: STRING
        >>')
      ELSE array()
    END AS D_records,

    current_timestamp() AS processedTime,
    _change_type,
    _commit_version,
    _commit_timestamp
  FROM parsed
)
KEYS (ID, BusinessID)
APPLY AS DELETE WHEN _change_type = "delete"
SEQUENCE BY (_commit_version, _commit_timestamp)
COLUMNS * EXCEPT (_change_type, _commit_version, _commit_timestamp)
STORED AS SCD TYPE 2;
