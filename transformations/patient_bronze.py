# import dlt
# from pyspark.sql.functions import col, current_timestamp, expr
# from pyspark.sql.types import StructType, StructField, StringType

# # ====================================================
# # 1️⃣ Bronze schema
# # ====================================================
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

# # ====================================================
# # 2️⃣ Bronze table
# # ====================================================
# @dlt.table(
#     name="bronze_events_patient_data",
#     comment="Raw TSV events - Bronze layer",
#     table_properties={
#         "quality": "bronze",
#         "delta.enableChangeDataFeed": "true",    # ✅ enables CDF
#         "delta.enableDeletionVectors": "true",
#         "delta.enableRowTracking": "true"
#     }
# )
# def bronze_events_patient_data():
#     path = "/Volumes/ankurnayyar_cat1/demo_schema/jsonschema"
#     bronze_df = (
#         spark.readStream.format("cloudFiles")
#         .option("cloudFiles.format", "csv")
#         .option("sep", "\t")
#         .option("header", "true")
#         .option("cloudFiles.schemaEvolutionMode", "rescue")
#         .option("cloudFiles.schemaLocation", "/Volumes/ankurnayyar_cat1/demo_schema/jsonschema_loc")
#         .schema(bronze_schema)
#         .load(path)
#     )

#     # Clean D column and parse as VARIANT
#     bronze_df = bronze_df.withColumn(
#         "D_clean",
#         expr(
#             """
#             CASE 
#                 WHEN D LIKE '"{%"}"' THEN 
#                     replace(
#                         regexp_replace(regexp_replace(substring(D,2,length(D)-2),'""','"'),'\\\\\"','"'),
#                         '\\u0000',''
#                     )
#                 ELSE replace(D,'\\u0000','')
#             END
#             """
#         )
#     )

#     return bronze_df.withColumn("ingestTime", current_timestamp())
#                    # .withColumn("D_variant", expr("parse_json(D_clean)"))