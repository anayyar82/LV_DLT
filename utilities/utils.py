from pyspark.sql.functions import *
from pyspark.sql.types import *

from pyspark.sql import DataFrame
from pyspark.sql.types import StructType, DataType,StringType,  IntegerType, DoubleType, DateType, TimestampType, TimestampNTZType, StringType
from typing import Dict, List, Union
import re
import dlt
import os
import pyarrow.parquet as pq
import pyarrow as pa

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
