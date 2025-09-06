# import dlt
# from pyspark.sql import functions as F
# from pyspark.sql.types import StructType, StringType, ArrayType, StructField
# from pyspark.sql import DataFrame

# # =========================================================
# # 🔧 Recursive flatten for Structs and Arrays
# # =========================================================
# def fully_flatten(df: DataFrame, struct_col_name="D_parsed", prefix="D_") -> DataFrame:
#     """
#     Recursively flattens all StructType columns and Array[StructType] columns.
#     Explodes arrays first, then flattens the resulting structs.
#     All column names are safe (dots replaced with underscores).
#     """
#     flatten_more = True
#     while flatten_more:
#         flatten_more = False
#         for field in df.schema.fields:
#             name = field.name
#             dtype = field.dataType

#             if isinstance(dtype, StructType):
#                 # Flatten struct fields
#                 for subfield in dtype.fields:
#                     sub_name = subfield.name
#                     col_name = f"{name}_{sub_name}"
#                     df = df.withColumn(col_name, F.col(f"{name}.{sub_name}"))
#                 df = df.drop(name)
#                 flatten_more = True

#             elif isinstance(dtype, ArrayType) and isinstance(dtype.elementType, StructType):
#                 # Explode array of structs
#                 df = df.withColumn(name + "_exploded", F.explode_outer(F.col(name)))
#                 df = df.drop(name)
#                 flatten_more = True

#     # Cast all remaining columns to string
#     for field in df.schema.fields:
#         df = df.withColumn(field.name, F.col(field.name).cast("string"))

#     return df

# # =========================================================
# # 1️⃣ Bronze Table
# # =========================================================
# bronze_schema = StructType([
#     StructField("ID", StringType(), True),
#     StructField("Shard", StringType(), True),
#     StructField("Private", StringType(), True),
#     StructField("Name", StringType(), True),
#     StructField("Address1", StringType(), True),
#     StructField("Address2", StringType(), True),
#     StructField("City", StringType(), True),
#     StructField("State", StringType(), True),
#     StructField("ZipCode", StringType(), True),
#     StructField("Country", StringType(), True),
#     StructField("PhoneNumber", StringType(), True),
#     StructField("BusinessID", StringType(), True),
#     StructField("Created", StringType(), True),
#     StructField("CreatedBy", StringType(), True),
#     StructField("Updated", StringType(), True),
#     StructField("UpdatedBy", StringType(), True),
#     StructField("V", StringType(), True),
#     StructField("D", StringType(), True),
#     StructField("P", StringType(), True)
# ])

# @dlt.table(
#     name="bronze_events_patient_data",
#     comment="Bronze ingestion of raw events",
#     table_properties={"quality": "bronze"}
# )
# def bronze_events_patient_data():
#     path = "/Volumes/ankurnayyar_cat1/demo_schema/jsonschema"

#     df = (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("sep", "\t")
#         .option("header", "true")
#         .option("cloudFiles.schemaEvolutionMode", "rescue")
#         .option("cloudFiles.schemaLocation", "/Volumes/ankurnayyar_cat1/demo_schema/jsonschema_loc")
#         .schema(bronze_schema)
#         .load(path)
#     )

#     # Clean JSON column D
#     df = df.withColumn(
#         "D_clean",
#         F.expr("""
#             CASE
#                 WHEN D IS NOT NULL THEN
#                     regexp_replace(substring(D, 2, length(D)-2), '""', '"')
#                 ELSE NULL
#             END
#         """)
#     )

#     return df.withColumn("ingestTime", F.current_timestamp()) \
#              .withColumn("_rescued_data", F.lit(None).cast("string"))

# # =========================================================
# # 2️⃣ Silver Flattened Table (Dynamic Columns)
# # =========================================================
# @dlt.table(
#     name="silver_events_patient_data",
#     comment="Silver flattened events with fully dynamic columns",
#     table_properties={"quality": "silver"}
# )
# @dlt.expect("business_id_not_null", "BusinessID IS NOT NULL")
# def silver_events_patient_data():
#     df = dlt.readStream("bronze_events_patient_data")

#     # Parse JSON with schema evolution (new keys added automatically)
#     df_parsed = df.withColumn(
#         "D_parsed",
#         F.from_json(
#             F.col("D_clean"),
#             schema=None,
#             options={
#                 "schemaLocationKey": "silver_events_patient_data_D",
#                 "schemaEvolutionMode": "addNewColumns"
#             }
#         )
#     )

#     # Dynamically flatten all keys from D_parsed
#     df_flat = fully_flatten(df_parsed, struct_col_name="D_parsed", prefix="D_")

#     return df_flat

# # =========================================================
# # 3️⃣ Silver SCD2 Table
# # =========================================================
# @dlt.table(
#     name="silver_events_patient_data_scd2",
#     comment="SCD2 table for historical tracking of D changes",
#     table_properties={"quality": "silver"}
# )
# def silver_events_patient_data_scd2():
#     df = dlt.readStream("silver_events_patient_data")

#     return df.withColumn("EffectiveFrom", F.col("ingestTime")) \
#              .withColumn("EffectiveTo", F.lit("9999-12-31").cast("date")) \
#              .withColumn("IsCurrent", F.lit(True))

# # =========================================================
# # 4️⃣ Error Records Table
# # =========================================================
# @dlt.table(
#     name="silver_events_patient_data_errors",
#     comment="Capture invalid JSON or bad records"
# )
# def silver_events_patient_data_errors():
#     df = dlt.readStream("bronze_events_patient_data")
#     return df.filter(F.col("D_clean").isNull() | (F.length("D_clean") < 5))
