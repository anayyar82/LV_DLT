import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, ArrayType
from pyspark.sql import DataFrame

# =========================================================
# Bronze Table (Raw JSON / CSV ingestion)
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
# Helper: Fully Recursive Flatten
# =========================================================
def fully_flatten(df: DataFrame) -> DataFrame:
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

    for field in df.schema.fields:
        df = df.withColumn(field.name, F.col(field.name).cast("string"))

    return df


# =========================================================
# Silver Flattened Table
# =========================================================
@dlt.table(
    name="silver_events_patient_data",
    comment="Silver flattened events with fully nested arrays and structs",
    table_properties={
        "quality": "silver",
        "delta.enableChangeDataFeed": "true",
        "delta.enableDeletionVectors": "true",
        "delta.enableRowTracking": "true"
    }
)
@dlt.expect("id_not_null", "ID IS NOT NULL")
@dlt.expect("business_id_not_null", "BusinessID IS NOT NULL")
@dlt.expect("phone_number_valid", "PhoneNumber RLIKE '^[0-9]{10,15}$'")
@dlt.expect("country_is_us", "Country = 'US'")
@dlt.expect("zip_valid", "ZipCode RLIKE '^[0-9]{5}$'")
@dlt.expect("state_valid", "State IS NOT NULL AND length(State) > 1")
def silver_events_patient_data():
    df = dlt.readStream("patient_cdf")

    df_flat = fully_flatten(df)

    # Drop any reserved CDC columns accidentally created
    for col_name in ["_commit_version", "_commit_timestamp", "_change_type"]:
        if col_name in df_flat.columns:
            df_flat = df_flat.drop(col_name)

    return df_flat


# =========================================================
# Silver CDC Table
# =========================================================
dlt.create_streaming_table("silver_events_patient_data_scd")

dlt.apply_changes(
    target="silver_events_patient_data_scd",
    source="silver_events_patient_data",
    keys=["ID", "BusinessID"],
    sequence_by="ingestTime",
    stored_as_scd_type=1
)


# =========================================================
# Quarantine Table for Failed Constraints
# =========================================================
@dlt.table(
    name="silver_events_patient_data_quarantine",
    comment="Rows violating expectations"
)
def silver_events_patient_data_quarantine():
    df = dlt.read("silver_events_patient_data")

    return df.filter(
        (F.col("ID").isNull()) |
        (F.col("BusinessID").isNull()) |
        (~F.col("PhoneNumber").rlike("^[0-9]{10,15}$")) |
        (F.col("Country") != "US") |
        (~F.col("ZipCode").rlike("^[0-9]{5}$")) |
        (F.col("State").isNull()) | (F.length("State") <= 1)
    ).withColumn(
        "quarantine_reason",
        F.when(F.col("ID").isNull(), "ID is NULL")
         .when(F.col("BusinessID").isNull(), "BusinessID is NULL")
         .when(~F.col("PhoneNumber").rlike("^[0-9]{10,15}$"), "Invalid PhoneNumber")
         .when(F.col("Country") != "US", "Invalid Country")
         .when(~F.col("ZipCode").rlike("^[0-9]{5}$"), "Invalid ZipCode")
         .when(F.col("State").isNull() | (F.length("State") <= 1), "Invalid State")
         .otherwise("Unknown Constraint Failure")
    )
