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
  D_parsed VARIANT,
  P_parsed VARIANT,
  V_parsed VARIANT,
  processedTime TIMESTAMP
)
TBLPROPERTIES (
  'delta.feature.variantType-preview' = 'supported',
  'pipelines.schemaInference' = 'true',
  'pipelines.autoOptimizeSchema' = 'true'
);

CREATE FLOW silver_events_patient_data_cdc AS AUTO CDC INTO
  silver_events_patient_data
FROM (
  SELECT
    ID, Shard, Private, Name, Address1, Address2, City, State,
    ZipCode, Country, PhoneNumber, BusinessID, Created, CreatedBy, Updated, UpdatedBy,
    from_json(
      regexp_replace(regexp_replace(substring(D,2,length(D)-2), '""','"'), '\\\\"','"'),
      NULL,
      map(
        'schemaLocationKey','silver_events_patient_data_D',
        'schemaHints','field1 STRING, field2 INT'
      )
    ) AS D_parsed,
    from_json(
      regexp_replace(regexp_replace(substring(P,2,length(P)-2), '""','"'), '\\\\"','"'),
      NULL,
      map(
        'schemaLocationKey','silver_events_patient_data_P',
        'schemaHints','field1 STRING, field2 INT'
      )
    ) AS P_parsed,
    from_json(
      regexp_replace(regexp_replace(substring(V,2,length(V)-2), '""','"'), '\\\\"','"'),
      NULL,
      map(
        'schemaLocationKey','silver_events_patient_data_V',
        'schemaHints','field1 STRING, field2 INT'
      )
    ) AS V_parsed,
    current_timestamp() AS processedTime,
    _change_type, _commit_version, _commit_timestamp
  FROM STREAM(patient_cdf)
)
KEYS (ID, BusinessID)
APPLY AS DELETE WHEN _change_type='delete'
APPLY AS TRUNCATE WHEN _change_type='truncate'
SEQUENCE BY (_commit_version,_commit_timestamp)
COLUMNS * EXCEPT (_change_type,_commit_version,_commit_timestamp)
STORED AS SCD TYPE 1;