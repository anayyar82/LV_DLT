-- ====================================================
-- SILVER PatientPractice SCD2 using AUTO CDC
-- ====================================================

-- Step 1: Stage table - parse JSON into struct with schema evolution
CREATE OR REFRESH STREAMING LIVE TABLE silver_patientpractice_stage
AS
SELECT
    PatientID,
    PracticeID,
    Shard,
    Created,
    CreatedBy,
    Updated,
    UpdatedBy,

    -- Parse JSON column D
    FROM_JSON(
        D,
        'STRUCT<
            address1: STRING,
            address2: STRING,
            anonymous: BOOLEAN,
            businessId: STRING,
            city: STRING,
            country: STRING,
            created: BIGINT,
            createdBy: STRING,
            name: STRING,
            patientId: STRING,
            phoneNumber: STRING,
            practiceId: STRING,
            referrerId: STRING,
            shard: BIGINT,
            state: STRING,
            updated: BIGINT,
            updatedBy: STRING,
            zipCode: STRING
        >',
        MAP(
            'mode', 'PERMISSIVE',
            'rescuedDataColumn', '_rescued_data',
            'schemaEvolutionMode', 'addNewColumns',
            'schemaLocationKey', 'silver_patientpractice_D'
        )
    ) AS D_struct
FROM LIVE.bronze_patientpractice_cdf;

-- Step 2: Apply AUTO CDC into Silver SCD2 table
CREATE OR REFRESH STREAMING LIVE TABLE silver_patientpractice_scd2
TBLPROPERTIES (
    'delta.enableChangeDataFeed' = 'true',
    'delta.enableDeletionVectors' = 'true',
    'delta.enableRowTracking' = 'true',
    'quality' = 'silver'
) AS
SELECT *
FROM LIVE.silver_patientpractice_stage
AUTO CDC
SEQUENCE BY (CURRENT_TIMESTAMP()) -- or a commit timestamp if available
STORED AS SCD TYPE 2
KEYS (PatientID, PracticeID);
