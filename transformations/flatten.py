# import dlt
# from pyspark.sql import functions as F
# from pyspark.sql.types import StringType, MapType

# # ====================================================
# # 1️⃣ Silver table read (already parsed D_extra)
# # ====================================================
# @dlt.table(
#     comment="Silver events with dynamic JSON keys captured in D_extra"
# )
# def silver_events_patient_data():
#     return dlt.read("silver_events_patient_data_scd2")

# # ====================================================
# # 2️⃣ Flatten each D_extra key as its own column
# # ====================================================
# @dlt.table(
#     comment="Flattened silver table with all dynamic keys as individual columns"
# )
# def silver_events_patient_data_flat():
#     df = dlt.read_stream("silver_events_patient_data")

#     # Step 1: Convert D_extra map to JSON string
#     df_with_json = df.withColumn("D_extra_json", F.to_json("D_extra"))

#     # Step 2: Parse JSON string into map type (all keys dynamically)
#     df_parsed = df_with_json.withColumn(
#         "D_extra_map", F.from_json("D_extra_json", MapType(StringType(), StringType()))
#     ).drop("D_extra_json")

#     # Step 3: Explode map into long format
#     df_long = df_parsed.withColumn("extra_kv", F.explode_outer("D_extra_map")) \
#                        .withColumn("extra_key", F.col("extra_kv").getItem(0)) \
#                        .withColumn("extra_value", F.col("extra_kv").getItem(1)) \
#                        .drop("extra_kv", "D_extra_map")

#     # Step 4: Convert long format back to wide with dynamic columns
#     df_wide = df_long.groupBy(
#         "ID", "BusinessID", "Shard", "Private", "Name", "Address1", "Address2",
#         "City", "State", "ZipCode", "Country", "PhoneNumber", "Created", "CreatedBy",
#         "Updated", "UpdatedBy",
#         "D_id","D_name","D_address1","D_address2","D_city","D_state","D_zipCode",
#         "D_country","D_phoneNumber","D_businessId","D_private","D_created","D_createdBy",
#         "D_updated","D_updatedBy","processedTime"
#     ).agg(
#         F.map_from_entries(
#             F.collect_list(F.struct(F.col("extra_key"), F.col("extra_value")))
#         ).alias("D_extra_map")
#     )

#     # Step 5: Flatten the map into columns dynamically
#     # This keeps all new keys as individual columns
#     df_final = df_wide
#     for key in df_wide.select(F.explode(F.map_keys("D_extra_map")).alias("k")).distinct().collect():
#         col_name = key["k"]
#         df_final = df_final.withColumn(col_name, F.col("D_extra_map")[col_name])

#     df_final = df_final.drop("D_extra_map")

#     return df_final

# # ====================================================
# # 3️⃣ Audit table to track schema evolution
# # ====================================================
# @dlt.table(
#     comment="Audit table tracking all new keys per record"
# )
# def dq_events_patient_data_keys1():
#     df = dlt.read_stream("silver_events_patient_data")

#     return df.select(
#         "ID", "BusinessID", "Shard",
#         F.explode_outer("D_extra").alias("kv"),
#         F.current_timestamp().alias("detected_at")
#     ).select(
#         "ID", "BusinessID", "Shard",
#         F.col("kv").getItem(0).alias("extra_key"),
#         F.col("kv").getItem(1).alias("extra_value"),
#         "detected_at"
#     ).filter(F.col("extra_key").isNotNull())
