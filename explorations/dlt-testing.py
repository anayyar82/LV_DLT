# Databricks notebook source
# MAGIC %md
# MAGIC https://community.databricks.com/t5/technical-blog/top-5-tips-to-build-delta-live-tables-dlt-pipelines-optimally/ba-p/83871

# COMMAND ----------

# from pyspark.sql.functions import *
# from pyspark.sql.types import *

# from pyspark.sql import DataFrame
# from pyspark.sql.types import StructType, DataType,StringType,  IntegerType, DoubleType, DateType, TimestampType, TimestampNTZType, StringType
# from typing import Dict, List, Union
# import re
# import dlt
# import os
# import pyarrow.parquet as pq
# import pyarrow as pa
from common_imports import *
from common_functions import *

# COMMAND ----------

# DBTITLE 1,View
@dlt.view(
  comment="The customers buying finished products, ingested from /databricks-datasets."
)
def vw_customers():
  return spark.read.csv('/databricks-datasets/retail-org/customers/customers.csv', header=True)

# COMMAND ----------

# DBTITLE 1,Read Custom Meta Data
def get_unique_custom_metadata(folder_path):
    """
    Extract unique custom metadata key-value pairs from parquet files in a directory.
    
    Args:
        folder_path (str): Path to the directory containing parquet files
        
    Returns:
        dict: Dictionary containing unique custom metadata keys and their values
    """
    custom_metadata = {}
    
    try:
        for filename in os.listdir(folder_path):
            if filename.endswith('.parquet'):
                file_path = os.path.join(folder_path, filename)
                metadata = pq.read_metadata(file_path)
                file_metadata = metadata.metadata
                
                # Add all custom_ key-value pairs to the dictionary
                for key, value in file_metadata.items():
                    decoded_key = key.decode('utf-8')
                    if decoded_key.startswith('custom.'):
                        decoded_value = value.decode('utf-8')
                        custom_metadata[decoded_key] = decoded_value
                        
        return custom_metadata
        
    except Exception as e:
        print(f"Error processing files: {str(e)}")
        raise
    finally:
        return custom_metadata

# COMMAND ----------

# DBTITLE 1,fn Get Table Properties (Bronze/Silver/Gold)
def get_table_properties(layer: str,custom_properties: dict = None) -> dict:
    """
    Returns table properties for specified layer with optional custom properties.
    
    Args:
        layer (str): 'bronze', 'silver', or 'gold'
        custom_properties (dict): Optional custom properties to override defaults
        
    Returns:
        dict: Combined table properties
    """
    try:
        # Base properties all tables share
        table_common_properties = {
            "pipelines.autoOptimize.managed": "true",
            "delta.autoOptimize.optimizeWrite": "true",
            "delta.tuneFileSizesForRewrites": "true",
            "delta.enableChangeDataFeed": "true",
            "delta.columnMapping.mode": "name",
            "delta.minReaderVersion": "2",
            "delta.minWriterVersion": "5",
            "delta.isolationLevel": "WriteSerializable"
        }
        
        # Layer-specific properties
        layer_properties = {
            "bronze": {
                "data.quality": "bronze"
                #"pipelines.reset.allowed": "false"
            },
            "silver": {
                "data.quality": "silver",
                # "delta.appendOnly": "false"
            },
            "gold": {
                "data.quality": "gold",
                # "delta.appendOnly": "true",
                # "delta.checkpointInterval": "10"
            }
        }
       
        # Combine properties
        properties = {
            **(custom_properties or {}),
            **table_common_properties,
            **layer_properties.get(layer, {})
        }

        return properties
    except Exception as e:
        print(f"Error in get_table_properties: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,fn Clean and Standarize Column Names
def clean_column_names(df: DataFrame) -> DataFrame:
    """
    Cleans column names by converting them to lowercase and replacing 
    non-alphanumeric characters with underscores.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with cleaned column names.
    """
    try:
        cleaned_columns = [col(col_name).alias(re.sub(r'\W+', '_', col_name.lower())) for col_name in df.columns]
        
        return df.select(*cleaned_columns)
    except Exception as e:
        print(f"Error in clean_column_names: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,fn Trim white spaces in Data
def trim_string_data(df: DataFrame) -> DataFrame:
    """
    Trims leading and trailing spaces from all string columns in the DataFrame.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The DataFrame with trimmed string data.
    """
    try:
        # Identify string columns
        string_columns = [col_name for col_name, dtype in df.dtypes if dtype == 'string']
        
        # Apply trim to the data in all string columns
        for col_name in string_columns:
            df = df.withColumn(col_name, trim(col(col_name)))
        
        return df
    except Exception as e:
        print(f"Error in trim_string_data: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,fn Convert specific column values to lowercase
def convert_data_to_lowercase(df: DataFrame, columns: list) -> DataFrame:
    """
    Converts the data in specified columns to lowercase.
    
    Args:
        df (DataFrame): The input DataFrame.
        columns (list): List of column names to convert to lowercase.

    Returns:
        DataFrame: The DataFrame with specified columns in lowercase.
    """
    try:
        # Apply lowercase transformation to the data in specified columns
        for col_name in columns:
            df = df.withColumn(col_name, lower(col(col_name)))
        
        return df
    except Exception as e:
        print(f"convert data to lowercase: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,fn Convert specific column values to titlecase
def convert_data_to_title_case(df: DataFrame, columns: list) -> DataFrame:
    """
    Converts the data in specified columns to title case 
    (capitalizing the first letter of each word).
    
    Args:
        df (DataFrame): The input DataFrame.
        columns (list): List of column names to convert to title case.

    Returns:
        DataFrame: The DataFrame with specified columns in title case.
    """
    try:
        # Apply title case transformation to the data in specified columns
        for col_name in columns:
            df = df.withColumn(col_name, initcap(col(col_name)))
        
        return df
    except Exception as e:
        print(f"convert data to titlecase: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,fn Standardize encoding to utf-8
def standardize_encoding(df: DataFrame, target_encoding: str = 'UTF-8') -> DataFrame:
    """
    Converts text data to the specified encoding (defaults to UTF-8).
    Also handles common encoding-related issues.
    
    Args:
        df (DataFrame): The input DataFrame.
        target_encoding (str): Target encoding format (default: 'UTF-8')
    Returns:
        DataFrame: The DataFrame with standardized encoding.
    """
    try:
        # Identify string columns
        string_columns = [field.name for field in df.schema.fields 
                         if isinstance(field.dataType, (StringType, VarcharType))]
        
        for column_name in string_columns:
            # Convert to binary, then to target encoding
            df = df.withColumn(
                column_name,
                encode(
                    when(col(column_name).isNotNull(), col(column_name))
                    .otherwise(lit("")),
                    target_encoding
                ).cast('string')
            )
            
            # Clean up any remaining encoding artifacts
            df = df.withColumn(
                column_name,
                regexp_replace(col(column_name), r'[\x00-\x1F\x7F-\xFF]', '')
            )
        
        return df
    except Exception as e:
        print(f"Error in standardize_encoding: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Generic Function invoking other functions
def standardize_dataframe(df: DataFrame) -> DataFrame:
    """
    Standardizes the DataFrame by cleaning column names, dropping duplicates, 
    adding audit columns, and trimming string data.
    
    Args:
        df (DataFrame): The input DataFrame.

    Returns:
        DataFrame: The standardized DataFrame.
    """
    try:
        # Clean the column names
        df = clean_column_names(df)

        # Drop duplicates
        df = df.dropDuplicates()
       
        # Trim string columns data
        df = trim_string_data(df)

        # Standardize the encoding
        df = standardize_encoding(df, 'UTF-8')
            
        return df
    
    except Exception as e:
        print(f"Error in standardize_dataframe: {str(e)}")
        raise

# COMMAND ----------

# DBTITLE 1,Bronze Level - JSON
@dlt.table(
  comment="The raw sales orders, ingested from /databricks-datasets.",
  table_properties=get_table_properties("bronze"),
  name="bronze_sales_orders"
)
def bronze_sales_orders():
  return (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.schemaLocation", "/tmp/myDemoPipeline/sales_order_schema/")
      .option("cloudFiles.format", "json")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "rescue")
      .load("/databricks-datasets/retail-org/sales_orders/")
      .withColumn("data_ingestion_ts", current_timestamp())
      .select("*",col("_metadata.file_path").alias("source_file"),col("_metadata.file_modification_time").alias("file_modification_time"))

  )

# COMMAND ----------

# DBTITLE 1,Bronze Level - Parquet
citibike_raw_data = "/Volumes/gannychan/rawdata/parquetfiles"
citibike_schema_location = "/tmp/myDemoPipeline/citibike_tripdata_schema/"
custom_properties = get_unique_custom_metadata(f"{citibike_raw_data}")

#print(custom_properties)

@dlt.table(
  name="bronze_citi_tripdata",
  comment="Citi Bike Trip Data from 2016.",
  table_properties=get_table_properties("bronze",custom_properties)
)
def bronze_citi_tripdata():
    df = (spark.readStream.format("cloudFiles")
      .option("cloudFiles.schemaLocation", f"{citibike_schema_location}")
      .option("cloudFiles.format", "parquet")
      .option("cloudFiles.inferColumnTypes", "true")
      .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
      .load(f"{citibike_raw_data}")
      .withColumn("data_ingestion_ts", current_timestamp())
    )
    
    # Clean all columns (lowercase and replaces spaces with underscore)
    df = (df.transform(clean_column_names)
            .select(
                coalesce(
                    col("tripduration"),
                    col("trip_duration"),
                ).alias("trip_duration"),
            
                coalesce(
                    col("starttime"),
                ).alias("start_time"),

                coalesce(
                    col("stoptime"),
                ).alias("stop_time"),

                coalesce(
                    col("start_station_id")
                ).alias("start_station_id"),

                coalesce(
                    col("start_station_name")
                ).alias("start_station_name"),

                coalesce(
                    col("start_station_latitude")
                ).alias("start_station_latitude"),

                coalesce(
                    col("start_station_longitude")
                ).alias("start_station_longitude"),

                coalesce(
                    col("end_station_id")
                ).alias("end_station_id"),

                coalesce(
                    col("end_station_name")
                ).alias("end_station_name"),

                coalesce(
                    col("end_station_latitude")
                ).alias("end_station_latitude"),

                coalesce(
                    col("end_station_longitude")
                ).alias("end_station_longitude"),
                
                coalesce(
                    col("bikeid"),
                    col("bike_id")
                ).alias("bike_id"),

                coalesce(
                    col("usertype"),
                    col("user_type")
                ).alias("user_type"),

                coalesce(
                    col("birth_year")
                ).alias("birth_year")
              )

    .select("*",col("_metadata.file_path").alias("source_file"),col("_metadata.file_modification_time").alias("file_modification_time"))

    )

    return df

# COMMAND ----------

# DBTITLE 1,Silver Level
@dlt.table(
  name="silver_sales_orders",
  comment="The cleaned sales orders with valid order_number(s) and partitioned by order_date",
  partition_cols=["order_date"],
  table_properties=get_table_properties("silver")
)

@dlt.expect_or_drop("valid order_number", "order_number IS NOT NULL")
def silver_sales_orders():
    try:
        df = spark.readStream.table("LIVE.bronze_sales_orders").join(dlt.read("vw_customers"), ["customer_id", "customer_name"], "left")

        df = df.withColumn("order_datetime", from_unixtime(df.order_datetime).cast("TIMESTAMP")) 
        df = df.withColumn("order_date", df.order_datetime.cast("DATE")) 
        df = df.select("customer_id", "customer_name", "number_of_line_items", "order_datetime", "order_date", "order_number", "ordered_products", "state", "city", "lon", "lat", "units_purchased", "loyalty_segment","source_file","data_ingestion_ts","file_modification_time")
        
        df.transform(standardize_dataframe)
        df.transform(convert_data_to_title_case, ["customer_name"])

        return df
    except:
        print(f"Error in silver_clean_sales_orders: {str(e)}")
        raise
  
  

# COMMAND ----------

# DBTITLE 1,Silver - CitiBike
df_appdata = spark.read.table("gannychan.dlt_demo.app_metadata")
schema = StructType([
    StructField("trip_duration", LongType(), True, {'comment': 'Duration of the Trip in Minutes'}),
    StructField("start_time", TimestampType(), True, {'comment': 'Trip Start Time'}),
    StructField("stop_time", TimestampType(), True, {'comment': 'Trip End Time'}),
    StructField("start_station_id", LongType(), True, {'comment': 'Starting Station ID'}),
    StructField("start_station_name", StringType(), True, {'comment': 'Starting Station Name'}),
    StructField("start_station_latitude", DoubleType(), True, {'comment': 'Starting Station Lat'}),
    StructField("start_station_longitude", DoubleType(), True, {'comment': 'Starting Station Lon'}),
    StructField("end_station_id", LongType(), True, {'comment': 'Destination Station ID'}),
    StructField("end_station_name", StringType(), True, {'comment': 'Destination Station Name'}),
    StructField("end_station_latitude", DoubleType(), True, {'comment': 'Destination Station Lat'}),
    StructField("end_station_longitude", DoubleType(), True, {'comment': 'Destination Station Lon'}),
    StructField("bike_id", LongType(), True, {'comment': 'Bike ID'}),
    StructField("user_type", StringType(), True, {'comment': 'User Type Subscriber / Customer'}),
    StructField("birth_year", DoubleType(), True, {'comment': 'Birth Year Integer'}),
    # StructField("data_ingestion_ts", TimestampType(), True, {'comment': 'Trip End Time'}),
    StructField("source_file", StringType(), True, {'comment': 'Trip End Time'}),
    StructField("file_modification_time", TimestampType(), True, {'comment': 'Trip End Time'}),
])


## Liquid Clustering

@dlt.table(
  name="silver_citi_tripdata",
  comment="The cleaned citibike",
  cluster_by=["start_station_id","user_type"],
  schema=schema,
  table_properties=get_table_properties("silver",custom_properties)
)

def silver_citi_tripdata():
    try:
        df = spark.readStream.table("LIVE.bronze_citi_tripdata")

        df = df.withColumn("start_time", to_timestamp(df.start_time,'M/d/yyyy HH:mm:ss'))
        df = df.withColumn("stop_time", to_timestamp(df.stop_time,'M/d/yyyy HH:mm:ss'))
        
        df.transform(standardize_dataframe)
        df.transform(convert_data_to_title_case, ["user_type"])

        return df
    except:
        print(f"Error in silver_citybike: {str(e)}")
        raise
  

# COMMAND ----------

# DBTITLE 1,Gold Level - 1
custom_properties = get_unique_custom_metadata("/Volumes/gannychan/rawdata/parquetfiles/")

@dlt.table(
  name="sales_order_in_la",
  comment="Aggregated sales orders in LA",
  table_properties=get_table_properties("gold")
)
def sales_order_in_la():
  df = spark.readStream.table("LIVE.silver_sales_orders").where("city == 'Los Angeles'") 

  df = df.select(df.city, df.order_date, df.customer_id, df.customer_name, explode(df.ordered_products).alias("ordered_products_explode"))

  dfAgg = (df.groupBy(df.order_date, df.city, df.customer_id, df.customer_name, df.ordered_products_explode.curr.alias("currency"))
    .agg(sum(df.ordered_products_explode.price).alias("sales"), sum(df.ordered_products_explode.qty).alias("quantity"))
  )

  return dfAgg

# COMMAND ----------

# DBTITLE 1,Gold Level - 2
@dlt.table(
  name="sales_order_in_chicago",
  comment="Sales orders in Chicago",
  table_properties=get_table_properties("gold")
)
def sales_order_in_chicago():
  df = spark.readStream.table("LIVE.silver_sales_orders").where("city == 'Chicago'")
  
  df = df.select(df.city, df.order_date, df.customer_id, df.customer_name, explode(df.ordered_products).alias("ordered_products_explode"))

  dfAgg = (df.groupBy(df.order_date, df.city, df.customer_id, df.customer_name, df.ordered_products_explode.curr.alias("currency"))
    .agg(sum(df.ordered_products_explode.price).alias("sales"), sum(df.ordered_products_explode.qty).alias("quantity"))
  )

  return dfAgg

# COMMAND ----------

# MAGIC %md
# MAGIC ### Limitations 
# MAGIC https://docs.databricks.com/en/delta-live-tables/unity-catalog.html#limitations
# MAGIC
# MAGIC - Data manipulation language (DML) queries that modify the schema of a streaming table are not supported.
# MAGIC - A materialized view created in a Delta Live Tables pipeline cannot be used as a streaming source outside of that pipeline, for example, in another pipeline or a downstream notebook.
# MAGIC - The LOCATION property is not supported when defining a table. (No External Tables)
# MAGIC - JARs are not supported. Only third-party Python libraries are supported 
# MAGIC - TAGS Support is not available.
# MAGIC

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### Recent Updates
# MAGIC
# MAGIC - Streaming Tables and Materialized Views can be accessed outside Pipeline using Singler User Cluster DBR 15.4
# MAGIC - Liquid Clustering can be enabled. (Preview Channel)
# MAGIC - ML Model can be built using Streaming Tables
