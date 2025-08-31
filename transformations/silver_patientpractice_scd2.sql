-- ====================================================
-- SILVER PatientPractice SCD2 — explicit schema, safe unescape, no invalid options
-- ====================================================

CREATE OR REFRESH STREAMING TABLE silver_patientpractice_scd2
TBLPROPERTIES (
  'delta.enableChangeDataFeed' = 'true',
  'delta.enableDeletionVectors' = 'true',
  'delta.enableRowTracking' = 'true',
  'delta.feature.variantType-preview' = 'supported',
  'quality' = 'silver'
) AS

WITH base AS (
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
    D
  FROM STREAM(bronze_patientpractice_cdf)
),
-- If D is double-quoted/escaped, clean it; otherwise pass through
clean AS (
  SELECT
    *,
    CASE
      WHEN D RLIKE '^(\\s*\\{|\\s*\\[)' THEN D
      WHEN D LIKE '"{%' THEN
        regexp_replace(
          regexp_replace(substr(D, 2, length(D)-2), '""', '"'),
          '\\\\\\"', '"'
        )
      ELSE D
    END AS D_clean
  FROM base
),
parsed AS (
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
    from_json(
      D_clean,
      -- derive schema from your example JSON (stable & explicit)
      schema_of_json('{
        "address1":"010 Dicki Union",
        "address2":"22025 Marlin Light",
        "anonymous":false,
        "businessId":"idxdlfxc71",
        "city":"North Nicolas",
        "country":"US",
        "created":1587214191,
        "createdBy":"17fdc071-8173-11ea-97b7-0242ac110008",
        "name":"practicevjalt6k7",
        "patientId":"17fdc071-8173-11ea-97b7-0242ac110008",
        "phoneNumber":"01653453370",
        "practiceId":"171ecdf0-8173-11ea-97b7-0242ac110008",
        "referrerId":"13f70c75-8173-11ea-844f-0242ac11000b",
        "shard":1836,
        "state":"Iowa",
        "updated":1587214191,
        "updatedBy":"17fdc071-8173-11ea-97b7-0242ac110008",
        "zipCode":"02665"
      }'),
      map('mode','PERMISSIVE', 'columnNameOfCorruptRecord','_corrupt_json')
    ) AS D_struct
  FROM clean
)
SELECT
  PatientID,
  PracticeID,
  Shard,
  Created,
  CreatedBy,
  Updated,
  UpdatedBy,

  -- Flattened from D
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
  to_timestamp(from_unixtime(D_struct.created)) AS D_created,
  D_struct.createdBy         AS D_createdBy,
  to_timestamp(from_unixtime(D_struct.updated)) AS D_updated,
  D_struct.updatedBy         AS D_updatedBy,

  current_timestamp() AS processedTime,

  -- CDC tech columns (useful if you attach an SCD2 flow later)
  _change_type,
  _commit_version,
  _commit_timestamp
FROM parsed;
