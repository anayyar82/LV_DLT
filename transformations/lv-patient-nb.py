# Databricks notebook source
from pyspark.sql.types import StructType, StructField, StringType

# COMMAND ----------

import dlt
from pyspark.sql.functions import col, current_timestamp, udf
from pyspark.sql import SparkSession
import re

spark = SparkSession.builder.getOrCreate()

"""
File Path Batch Parsing
"""
pattern = r"\d{4}/\d+/\d+/[\d\-]+"

@udf
def extract_batch_udf(file_path):
    res = re.search(pattern, file_path)
    return None if not res else res.group()

def generate_bronze_table(path, schema, table_name):
    @dlt.table(
        name=f"bronze_{table_name}",
        table_properties={
            "quality": "bronze",
            "delta.enableChangeDataFeed": "true",   # ✅ enables CDF
            "delta.enableDeletionVectors": "true",
            "delta.enableRowTracking": "true"
        }
    )
    def read_file():
        return (
            spark.readStream.format("cloudFiles")
            .option("cloudFiles.format", "csv")
            .option("sep", "\t")
            .option("header", "true")
            .schema(schema)
            .load(path)
            .withColumn("inputFilename", col("_metadata.file_name"))
            .withColumn("fullFilePath", col("_metadata.file_path"))
            .withColumn("fileMetadata", col("_metadata"))
            .withColumn("bronze_prefix", extract_batch_udf(col("_metadata").getField("file_path")))
            .select(
                col("*"),
                current_timestamp().alias("ingestTime"),
                current_timestamp().cast("date").alias("ingestDate")
            )
        )


# COMMAND ----------

raw_data_path = "s3://adc-lv-dev-devops.libreview-data-migration"
catalog_name = "dev-adc-superiorlv-catalog-us-east-2"
database_name = "lv-patient"
gdprbacklog_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Processed", StringType(), True),
        StructField("Active", StringType(), True),
        StructField("Timestamp", StringType(), True),
    ]
)
partnernotification_schema = StructType(
    [
        StructField("ID", StringType(), True),
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("PartnerID", StringType(), True),
        StructField("Retry", StringType(), True),
        StructField("Message", StringType(), True),
    ]
)
patientsensor_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("SerialNumber", StringType(), True),
        StructField("Activated", StringType(), True),
        StructField("Ended", StringType(), True),
        StructField("D", StringType(), True),
        StructField("Created", StringType(), True),
    ]
)
patientpartnerconnection_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("PartnerID", StringType(), True),
        StructField("Created", StringType(), True),
        StructField("CreatedBy", StringType(), True),
        StructField("Updated", StringType(), True),
        StructField("UpdatedBy", StringType(), True),
        StructField("V", StringType(), True),
        StructField("D", StringType(), True),
        StructField("P", StringType(), True),
    ]
)
patientconnectionhistory_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("FollowerID", StringType(), True),
        StructField("Action", StringType(), True),
        StructField("Status", StringType(), True),
        StructField("D", StringType(), True),
        StructField("Updated", StringType(), True),
    ]
)
measurement_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("DeviceID", StringType(), True),
        StructField("Timestamp", StringType(), True),
        StructField("Type", StringType(), True),
        StructField("FactoryTimestamp", StringType(), True),
        StructField("Created", StringType(), True),
        StructField("D", StringType(), True),
        StructField("UploadID", StringType(), True),
        StructField("RecordNumber", StringType(), True),
        StructField("TimestampOffset", StringType(), True),
    ]
)
patientconnection_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("FollowerID", StringType(), True),
        StructField("S", StringType(), True),
        StructField("SUpdated", StringType(), True),
        StructField("R", StringType(), True),
        StructField("RUpdated", StringType(), True),
        StructField("D", StringType(), True),
        StructField("Updated", StringType(), True),
    ]
)
patientupload_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("UploadID", StringType(), True),
        StructField("DeviceID", StringType(), True),
        StructField("DeviceTypeID", StringType(), True),
        StructField("TSMin", StringType(), True),
        StructField("TSMax", StringType(), True),
        StructField("FTSMin", StringType(), True),
        StructField("FTSMax", StringType(), True),
        StructField("Created", StringType(), True),
    ]
)
librelinksession_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("DeviceID", StringType(), True),
        StructField("DeviceTypeID", StringType(), True),
        StructField("Created", StringType(), True),
        StructField("CreatedBy", StringType(), True),
        StructField("Updated", StringType(), True),
        StructField("UpdatedBy", StringType(), True),
        StructField("V", StringType(), True),
        StructField("D", StringType(), True),
        StructField("P", StringType(), True),
    ]
)
patientpractice_schema = StructType(
    [
        StructField("PatientID", StringType(), True),
        StructField("Shard", StringType(), True),
        StructField("PracticeID", StringType(), True),
        StructField("Created", StringType(), True),
        StructField("CreatedBy", StringType(), True),
        StructField("Updated", StringType(), True),
        StructField("UpdatedBy", StringType(), True),
        StructField("V", StringType(), True),
        StructField("D", StringType(), True),
        StructField("P", StringType(), True),
    ]
)

# COMMAND ----------

tables = ["GdprBacklog", "PartnerNotification", "PatientSensor", "PatientPartnerConnection", "PatientConnectionHistory", "Measurement", "PatientConnection", "PatientUpload", "LibreLinkSession", "PatientPractice"]
schemas = [gdprbacklog_schema, partnernotification_schema, patientsensor_schema, patientpartnerconnection_schema, patientconnectionhistory_schema, measurement_schema, patientconnection_schema, patientupload_schema, librelinksession_schema, patientpractice_schema]
for table, schema in zip(tables, schemas):
    file_path = f"{raw_data_path}/patient-1_{table}"
    generate_bronze_table(file_path, schema, table)
