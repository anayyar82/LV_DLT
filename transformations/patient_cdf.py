# import dlt

# @dlt.view()
# def patient_cdf():
#   df = spark.readStream.option("readChangeFeed", "true").table(f"bronze_events_patient_data")
#   return df