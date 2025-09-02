-- -- ====================================================

-- --  Defining a Silver SCD2 table with:
-- -- ====================================================


-- -- All known D fields flattened

-- -- Catch-all map for any unknown keys (D_extra)

-- -- Structured records (D_records)

-- -- Support for P and V JSON maps

-- -- SCD2 applied with KEYS and SEQUENCE

-- -- DLT expectations after table definition:

-- -- ID must not be null

-- -- BusinessID must not be null

-- -- D_created must not be null

-- -- D_state must be 2 characters (like US state codes)

-- -- D_zipCode must match US ZIP pattern

-- SET spark.databricks.delta.schema.autoMerge.enabled = true;

-- -- ====================================================
-- -- 1️⃣ Base Silver Table with structured D_records, D_extra, P, and V
-- -- ====================================================
-- -- ====================================================
-- -- Silver Patient Data SCD2 with constraints
-- -- ====================================================
-- CREATE OR REFRESH STREAMING TABLE silver_events_patient_data_scd2
-- (
--   ID STRING,
--   Shard STRING,
--   Private STRING,
--   Name STRING,
--   Address1 STRING,
--   Address2 STRING,
--   City STRING,
--   State STRING,
--   ZipCode STRING,
--   Country STRING,
--   PhoneNumber STRING,
--   BusinessID STRING,
--   Created STRING,
--   CreatedBy STRING,
--   Updated STRING,
--   UpdatedBy STRING,

--   -- Flattened known fields
--   D_id STRING,
--   D_name STRING,
--   D_address1 STRING,
--   D_address2 STRING,
--   D_city STRING,
--   D_state STRING,
--   D_zipCode STRING,
--   D_country STRING,
--   D_phoneNumber STRING,
--   D_businessId STRING,
--   D_private BOOLEAN,
--   D_created TIMESTAMP,
--   D_createdBy STRING,
--   D_updated TIMESTAMP,
--   D_updatedBy STRING,

--   -- Catch-all map for unknown keys
--   D_extra MAP<STRING, STRING>,

--   -- Array of structured records
--   D_records ARRAY<STRUCT<
--       record_id: STRING,
--       record_name: STRING,
--       option_checkBoxes: ARRAY<STRING>,
--       option_data: STRING,
--       option_fixedText: BOOLEAN,
--       priority: INT,
--       type: STRING,
--       record_extra: MAP<STRING, STRING>
--   >>,

--   -- JSON maps for P and V
--   P STRING,
--   V STRING,

--   processedTime TIMESTAMP,

--   -- ====================================================
--   -- 🔎 Constraints for Data Quality with actions
--   -- ====================================================
--   CONSTRAINT id_not_null           EXPECT (ID IS NOT NULL), --ON VIOLATION DROP ROW,
--   CONSTRAINT businessid_not_null   EXPECT (BusinessID IS NOT NULL), --ON VIOLATION DROP ROW,
--   CONSTRAINT dcreated_not_null     EXPECT (D_created IS NOT NULL), -- ON VIOLATION DROP ROW,
--   CONSTRAINT state_two_chars       EXPECT (D_state RLIKE '^[A-Z]{2}$'), 
--   CONSTRAINT zipcode_us_pattern    EXPECT (D_zipCode RLIKE '^[0-9]{5}(-[0-9]{4})?$'))
-- TBLPROPERTIES (
--   'delta.enableChangeDataFeed'='true',
--   'delta.enableRowTracking'='true',
--   'quality'='silver'
-- );

-- -- ====================================================
-- -- 2️⃣ Parse and load records dynamically into Silver table
-- -- ====================================================
-- CREATE FLOW silver_events_patient_data_cdc_scd2 AS AUTO CDC INTO
--   silver_events_patient_data_scd2
-- FROM (
--   WITH cleaned AS (
--     SELECT *,
--            CASE WHEN D LIKE '"{%"}"' THEN
--              replace(
--                regexp_replace(regexp_replace(substring(D,2,length(D)-2),'""','"'),'\\\\\"','"'),
--                '\\u0000',''
--              )
--            ELSE replace(D,'\\u0000','')
--            END AS D_clean
--     FROM STREAM(patient_cdf)
--   ),
--   parsed AS (
--     SELECT *,
--            from_json(D_clean,'MAP<STRING,STRING>') AS D_map
--     FROM cleaned
--   ),
--   records_structured AS (
--     SELECT *,
--            CASE WHEN D_map['records'] IS NOT NULL THEN
--              transform(
--                from_json(D_map['records'],'ARRAY<MAP<STRING,STRING>>'),
--                r -> struct(
--                  r['id'] AS record_id,
--                  r['name'] AS record_name,
--                  CASE WHEN r['optionData.checkBoxes'] IS NOT NULL THEN
--                       from_json(r['optionData.checkBoxes'],'ARRAY<STRING>')
--                  ELSE array() END AS option_checkBoxes,
--                  r['optionData.data'] AS option_data,
--                  CAST(r['optionData.fixedText'] AS BOOLEAN) AS option_fixedText,
--                  CAST(r['priority'] AS INT) AS priority,
--                  r['type'] AS type,
--                  map_filter(r,(k,v)->k NOT IN ('id','name','optionData','priority','type')) AS record_extra
--                )
--              )
--            ELSE array() END AS D_records
--     FROM parsed
--   )
--   SELECT
--     ID, Shard, Private, Name, Address1, Address2, City, State, ZipCode, Country,
--     PhoneNumber, BusinessID, Created, CreatedBy, Updated, UpdatedBy,

--     -- Known fields
--     D_map['id'] AS D_id,
--     D_map['name'] AS D_name,
--     D_map['address1'] AS D_address1,
--     D_map['address2'] AS D_address2,
--     D_map['city'] AS D_city,
--     D_map['state'] AS D_state,
--     D_map['zipCode'] AS D_zipCode,
--     D_map['country'] AS D_country,
--     D_map['phoneNumber'] AS D_phoneNumber,
--     D_map['businessId'] AS D_businessId,
--     CAST(D_map['private'] AS BOOLEAN) AS D_private,
--     to_timestamp(D_map['created']) AS D_created,
--     D_map['createdBy'] AS D_createdBy,
--     to_timestamp(D_map['updated']) AS D_updated,
--     D_map['updatedBy'] AS D_updatedBy,

--     -- Catch-all extra keys
--     map_filter(D_map,(k,v)->k NOT IN (
--       'id','name','address1','address2','city','state','zipCode','country',
--       'phoneNumber','businessId','private','created','createdBy','updated','updatedBy',
--       'records','P','V'
--     )) AS D_extra,

--     -- Structured records
--     D_records,
--     P, V,
--     -- Parse P and V JSON as maps
--     --CASE WHEN D_map['P'] IS NOT NULL THEN from_json(D_map['P'], 'MAP<STRING,STRING>') END AS P,
--     --CASE WHEN D_map['V'] IS NOT NULL THEN from_json(D_map['V'], 'MAP<STRING,STRING>') END AS V,

--     current_timestamp() AS processedTime,
--     _change_type, _commit_version, _commit_timestamp
--   FROM records_structured
-- )
-- KEYS (ID,BusinessID)
-- APPLY AS DELETE WHEN _change_type='delete'
-- SEQUENCE BY (_commit_version,_commit_timestamp)
-- COLUMNS * EXCEPT (_change_type,_commit_version,_commit_timestamp)
-- STORED AS SCD TYPE 2;
