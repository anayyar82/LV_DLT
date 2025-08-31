# import dlt
# from pyspark.sql.functions import col, current_timestamp, udf, from_json
# from pyspark.sql.types import StructType
# from pyspark.sql import SparkSession
# import re
# import yaml

# spark = SparkSession.builder.getOrCreate()

# """
# File Path Batch Parsing
# """
# pattern = "\d{4}/\d+/\d+/[\d\-]+"

# @udf
# def extract_batch_udf(file_path):

#     res = re.search(pattern, file_path)

#     return None if not res else res.group()

# def schema_contains_column(schema: StructType, column_name: str):
#     """
#     This function checks if a schema contains a column with the given name
#     """
#     if not isinstance(schema, StructType):
#         raise ValueError("schema must be a StructType")
#     return column_name in schema.fieldNames()

# def get_config(path:str):
#     try:
#         with open(path, 'r') as file:
#             config = yaml.safe_load(file)
#         return config
#     except FileNotFoundError:
#         print(f"File not found: {path}")
#     except yaml.YAMLError as e:
#         print(f"Error parsing YAML: {path}: {e}")
#     except Exception as e:
#         print(f"Unexpected error: {path}: {e}")

# def generate_bronze_table(path, schema, table_name, table_hint):
#     if schema_contains_column(schema, "D"):
#         @dlt.table(
#             name=f"bronze_{table_name}"
#         )
#         def read_file():
#             try:
#                 return (
#                     spark.readStream.format("cloudFiles")
#                     .option("cloudFiles.format", "csv")
#                     .option("sep", "\t")
#                     .option("header", "false")
#                     .option("multiLine", "true")
#                     .option("encoding", "UTF-16LE")
#                     .schema(schema)
#                     .load(path)
#                     .withColumn("inputFilename", col("_metadata.file_name"))
#                     .withColumn("fullFilePath", col("_metadata.file_path"))
#                     .withColumn("fileMetadata", col("_metadata"))
#                     .withColumn("bronze_prefix", extract_batch_udf(col("_metadata").getField("file_path")))
#                     .select( 
#                         col("*")
#                         ,current_timestamp().alias("ingestTime")
#                         ,current_timestamp().cast("date").alias("ingestDate")
#                     )
#                     .withColumn(
#                         "D_parsed",
#                         from_json(
#                             col("D").cast('string'),
#                             None,
#                             {"schemaLocationKey": table_name,
#                              'schemaHints': f'_corrupt_record STRING{table_hint}',
#                              'columnNameOfCorruptRecord': '_corrupt_record'}
#                         )
#                         )
#                 )
#             except Exception as e:
#                 print(f"Error reading file:, {e}")
    
#     else:
#         @dlt.table(
#             name=f"bronze_{table_name}"
#         )
#         def read_file():
#             try:
#                 return (
#                     spark.readStream.format("cloudFiles")
#                     .option("cloudFiles.format", "csv")
#                     .option("sep", "\t")
#                     .option("header", "false")
#                     .option("multiLine", "true")
#                     .option("encoding", "UTF-16LE")
#                     .schema(schema)
#                     .load(path)
#                     .withColumn("inputFilename", col("_metadata.file_name"))
#                     .withColumn("fullFilePath", col("_metadata.file_path"))
#                     .withColumn("fileMetadata", col("_metadata"))
#                     .withColumn("bronze_prefix", extract_batch_udf(col("_metadata").getField("file_path")))
#                     .select( 
#                         col("*")
#                         ,current_timestamp().alias("ingestTime")
#                         ,current_timestamp().cast("date").alias("ingestDate")
#                     )
#                 )
#             except Exception as e:
#                 print(f"Error reading file:, {e}")