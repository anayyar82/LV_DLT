# import dlt
# from pyspark.sql.functions import col, lit, count

# # list of tables you want CDF enabled on
# tables = [
#     "GdprBacklog",
#     "PartnerNotification",
#     "PatientSensor",
#     "PatientPartnerConnection",
#     "PatientConnectionHistory",
#     "Measurement",
#     "PatientConnection",
#     "PatientUpload",
#     "LibreLinkSession",
#     "PatientPractice"
# ]

# # dynamically create CDF views for each bronze table
# for tbl in tables:
#     @dlt.view(name=f"bronze_{tbl.lower()}_cdf")
#     def _make_view(tbl=tbl):  # bind loop variable
#         return (
#             spark.readStream
#             .option("readChangeFeed", "true")
#             .table(f"bronze_{tbl.lower()}")
#         )

# # build a commits aggregation across all bronze cdf views
# @dlt.table(name="bronze_commits")
# def bronze_commits():
#     dfs = []
#     for tbl in tables:
#         df = (
#             spark.readStream.table(f"bronze_{tbl.lower()}_cdf")
#             .select(
#                 col("_change_type").alias("change_type"),
#                 col("_commit_version").alias("commit_version"),
#                 col("_commit_timestamp").alias("commit_timestamp"),
#             )
#             .withColumn("table_name", lit(tbl))
#         )
#         dfs.append(df)

#     # union all commit dfs together
#     union_df = dfs[0]
#     for df in dfs[1:]:
#         union_df = union_df.unionByName(df)

#     return union_df.groupBy(
#         "table_name", "change_type", "commit_version", "commit_timestamp"
#     ).agg(
#         count("*").alias("rcrd_cnt")   # ✅ same as SQL: count(*) as rcrd_cnt
#     )
