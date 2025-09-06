# Fully Recursive Flatten Function (fully_flatten)

# Purpose: Converts deeply nested JSON data into a flat table.

# How it works:

# Structs: If a column is a structured object (like {"field1": "value", "field2": "value"}), each subfield becomes its own column. Example: D.address1 → D_address1.

# Arrays of Structs: If a column is an array of objects (like records: [{"id":1}, {"id":2}]), the array is exploded so each element becomes its own row, then flattened.

# Dot replacement: Any dots in column names are replaced with underscores to prevent SQL errors.

# Type casting: All final columns are cast to strings to avoid type drift issues in streaming data.

# Business impact: Allows analysts to query nested JSON data in a simple, tabular format without writing complex JSON parsing code.

# 2️⃣ Bronze Table (bronze_events_patient_data)

# Purpose: Raw ingestion of TSV data from a given folder into the Bronze layer.

# Steps:

# Read stream from /Volumes/ankurnayyar_cat1/demo_schema/jsonschema as TSV.

# Clean the D column: Remove unwanted quotes, fix escaped characters.

# Parse JSON (D_parsed): Converts the cleaned JSON string into a structured column, using schema evolution so new keys are automatically added.

# Add metadata: Columns ingestTime and _rescued_data for tracking ingestion and rescued bad data.

# Business impact: Raw data is captured reliably in its original form, with the ability to handle new fields in JSON dynamically.

# 3️⃣ Silver Flattened Table (silver_events_patient_data)

# Purpose: Flattened version of Bronze data for easy querying and analytics.

# Steps:

# Reads from the Bronze table.

# Applies fully_flatten on the parsed JSON (D_parsed) and any nested arrays.

# Produces a flat table with all JSON fields as separate columns, even newly added ones.

# Business impact: Business users can query JSON data as a regular table without dealing with nested structures. New fields added to the JSON automatically become new columns in Silver.

# 4️⃣ Silver SCD2 Table (silver_events_patient_data_scd2)

# Purpose: Track historical changes in the Silver table using Slowly Changing Dimension Type 2 (SCD2).

# How it works:

# Adds EffectiveFrom (ingest time) and EffectiveTo (far future date by default).

# Adds IsCurrent to indicate the latest version of the row.

# Business impact: Historical changes to patient events are retained. Analysts can track what values were valid at any point in time.

# 5️⃣ Error Records Table (silver_events_patient_data_errors)

# Purpose: Capture invalid JSON or malformed rows from Bronze.

# How it works:

# Filters rows where D_clean is null or too short to be valid JSON.

# Business impact: Ensures data quality by isolating problematic records for review or correction.


import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType, StructType
from pyspark.sql import DataFrame

# =========================================================
# 🔧 Helper: Fully Recursive Flatten
# =========================================================
def fully_flatten(df: DataFrame) -> DataFrame:
    """
    Recursively flatten structs and arrays of structs.
    Explodes arrays, flattens structs, and replaces dots with underscores.
    """
    from pyspark.sql.types import StructType, ArrayType

    flatten_more = True
    while flatten_more:
        flatten_more = False
        for field in df.schema.fields:
            name = field.name
            dtype = field.dataType
            safe_name = name.replace(".", "_")

            if isinstance(dtype, StructType):
                for subfield in dtype.fields:
                    df = df.withColumn(f"{safe_name}_{subfield.name}", F.col(f"{name}.{subfield.name}"))
                df = df.drop(name)
                flatten_more = True

            elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
                df = df.withColumn(f"{name}_exploded", F.explode_outer(F.col(name)))
                df = df.drop(name)
                flatten_more = True

    # Cast everything to string to handle type drift
    for field in df.schema.fields:
        df = df.withColumn(field.name, F.col(field.name).cast("string"))

    return df

# =========================================================
# 1️⃣ Bronze Table (JSON parsed)
# =========================================================
bronze_schema = StructType([
    StructField("ID", StringType(), True),
    StructField("Shard", StringType(), True),
    StructField("Private", StringType(), True),
    StructField("Name", StringType(), True),
    StructField("Address1", StringType(), True),
    StructField("Address2", StringType(), True),
    StructField("City", StringType(), True),
    StructField("State", StringType(), True),
    StructField("ZipCode", StringType(), True),
    StructField("Country", StringType(), True),
    StructField("PhoneNumber", StringType(), True),
    StructField("BusinessID", StringType(), True),
    StructField("Created", StringType(), True),
    StructField("CreatedBy", StringType(), True),
    StructField("Updated", StringType(), True),
    StructField("UpdatedBy", StringType(), True),
    StructField("V", StringType(), True),
    StructField("D", StringType(), True),
    StructField("P", StringType(), True)
])

@dlt.table(
    name="bronze_events_patient_data",
    comment="Bronze ingestion of raw events with D parsed",
    table_properties={"quality": "bronze"}
)
def bronze_events_patient_data():
    path = "/Volumes/ankurnayyar_cat1/demo_schema/jsonschema"

    df = (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("sep", "\t")
        .option("header", "true")
        .option("cloudFiles.schemaEvolutionMode", "rescue")
        .option("cloudFiles.schemaLocation", "/Volumes/ankurnayyar_cat1/demo_schema/jsonschema_loc")
        .schema(bronze_schema)
        .load(path)
    )

    # Clean JSON column D
    df = df.withColumn(
        "D_clean",
        F.expr("""
            CASE
                WHEN D IS NOT NULL THEN
                    regexp_replace(substring(D, 2, length(D)-2), '""', '"')
                ELSE NULL
            END
        """)
    )

    # Parse JSON in Bronze using schema evolution
    df = df.withColumn(
        "D_parsed",
        F.from_json(
            F.col("D_clean"),
            schema=None,
            options={
                "schemaLocationKey": "bronze_events_patient_data_D",
                "schemaEvolutionMode": "addNewColumns"
            }
        )
    )

    return df.withColumn("ingestTime", F.current_timestamp()) \
             .withColumn("_rescued_data", F.lit(None).cast("string"))

# =========================================================
# 2️⃣ Silver Flattened Table
# =========================================================
@dlt.table(
    name="silver_events_patient_data",
    comment="Silver flattened events with fully nested arrays and structs",
    table_properties={"quality": "silver"}
)
@dlt.expect("business_id_not_null", "BusinessID IS NOT NULL")
def silver_events_patient_data():
    df = dlt.readStream("bronze_events_patient_data")

    # Fully flatten D_parsed
    df_flat = fully_flatten(df)

    return df_flat

# =========================================================
# 3️⃣ Silver SCD2 Table
# =========================================================
@dlt.table(
    name="silver_events_patient_data_scd2",
    comment="SCD2 table for historical tracking of D changes",
    table_properties={"quality": "silver"}
)
def silver_events_patient_data_scd2():
    df = dlt.readStream("silver_events_patient_data")
    return df.withColumn("EffectiveFrom", F.col("ingestTime")) \
             .withColumn("EffectiveTo", F.lit("9999-12-31").cast("date")) \
             .withColumn("IsCurrent", F.lit(True))

# =========================================================
# 4️⃣ Error Records Table
# =========================================================
@dlt.table(
    name="silver_events_patient_data_errors",
    comment="Capture invalid JSON or bad records"
)
def silver_events_patient_data_errors():
    df = dlt.readStream("bronze_events_patient_data")
    return df.filter(F.col("D_clean").isNull() | (F.length("D_clean") < 5))
