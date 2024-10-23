PySpark Business Use Cases in the Telecommunication Sector
This document presents business use cases for PySpark in the telecommunication sector using HDFS as the source and Hive as the target. We will explore data cleaning techniques such as removing duplicates, handling null values, and applying other popular data cleansing methods. The sample data involves multiple entities, and we will transform this data into multiple tables in Hive using PySpark code.
1. Business Use Cases
1.1 Call Detail Records (CDR) Analysis
**Description**: Analyzing call detail records to identify patterns in call durations, dropped calls, and network issues. This involves handling inconsistencies, missing values, and removing duplicate records for accurate analysis.
1.2 Customer Usage Data Integration
**Description**: Integrating customer usage data from various sources to create a unified and consistent dataset. This includes handling missing values, removing duplicates, standardizing data formats, and ensuring data completeness.
2. Sample Data
2.1 Call Detail Records (CDR) Data (HDFS Source)
**Columns**: call_id, customer_id, call_start_time, call_end_time, call_duration, call_type, network_type, call_status
**Sample Records**:
101, 1001, 2024-09-01 10:00:00, 2024-09-01 10:05:00, 300, outgoing, 4G, completed
102, 1002, 2024-09-01 11:00:00, 2024-09-01 11:10:00, 600, incoming, 4G, completed
103, 1002, 2024-09-01 11:00:00, 2024-09-01 11:10:00, 600, incoming, 4G, completed
104, 1003, 2024-09-01 12:00:00, NULL, NULL, outgoing, 3G, dropped
2.2 Customer Usage Data (HDFS Source)
**Columns**: usage_id, customer_id, data_used_gb, voice_minutes_used, sms_used, billing_cycle_start, billing_cycle_end, total_charges
**Sample Records**:
201, 1001, 5.2, 150, 20, 2024-08-01, 2024-08-31, 50.00
202, 1002, 3.5, 200, 50, 2024-08-01, 2024-08-31, 40.00
203, 1002, 3.5, 200, 50, 2024-08-01, 2024-08-31, 40.00
204, 1003, NULL, NULL, 0, 2024-08-01, 2024-08-31, 30.00

*********************************************************************************************************************************************************************************************
Solution

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, lit, when, trim, lower
from pyspark.sql.functions import to_timestamp


def telecommunication_py():
    # Initialize Spark Session
    spark = SparkSession.builder.appName("TelecomDataProcessing").enableHiveSupport().getOrCreate()

    # Read Call Detail Records (CDR) Data from HDFS
    cdr_df = spark.read.csv("hdfs:///user/test/telecom/call_details.txt", header=True, inferSchema=True)

    # Read Customer Usage Data from HDFS
    usage_df = spark.read.csv("hdfs:///user/test/telecom/customer_usage.txt", header=True, inferSchema=True)

    # Data Cleaning: Remove Duplicates
    cdr_df = cdr_df.dropDuplicates()
    usage_df = usage_df.dropDuplicates()

    # Convert call_end_time to timestamp
    print(cdr_df.dtypes)
    cdr_df = cdr_df.toDF(*[col.strip() for col in cdr_df.columns])
    usage_df = usage_df.toDF(*[col.strip() for col in usage_df.columns])
    # Replace 'NULL' and empty strings in call_end_time with None
    cdr_df = cdr_df.replace(['NULL', ''], None, subset=['call_end_time', 'call_duration', 'call_status'])

    cdr_df = cdr_df.withColumn('call_duration', col('call_duration').cast('int'))


    # Handle Null Values: Replace NULLs with Default Values
    cdr_df = cdr_df.fillna({"call_end_time": "unknown", "call_duration": 0, "call_status": "incomplete"})
    usage_df = usage_df.fillna({"data_used_gb": 0.0, "voice_minutes_used": 0, "sms_used": 0})

    # Standardize Data Formats: Trim Whitespaces and Convert to Lowercase
    cdr_df = cdr_df.withColumn("call_type", trim(lower(col("call_type"))))
    usage_df = usage_df.withColumn("total_charges",
                                   when(col("total_charges").isNull(), lit(0.0)).otherwise(col("total_charges")))


    # Write Cleaned Data to Hive Tables
    cdr_df.write.mode("overwrite").saveAsTable("telecom_db.cleaned_cdr_data")
    usage_df.write.mode("overwrite").saveAsTable("telecom_db.cleaned_usage_data")

    # Check Record Counts Between Source and Target
    source_cdr_count = cdr_df.count()
    target_cdr_count = spark.sql("SELECT COUNT(*) FROM telecom_db.cleaned_cdr_data").collect()[0][0]

    if source_cdr_count == target_cdr_count:
        print("CDR data is complete and consistent.")
    else:
        print("Data inconsistency detected in CDR records.")

    source_usage_count = usage_df.count()
    target_usage_count = spark.sql("SELECT COUNT(*) FROM telecom_db.cleaned_usage_data").collect()[0][0]

    if source_usage_count == target_usage_count:
        print("Customer usage data is complete and consistent.")
    else:
        print("Data inconsistency detected in customer usage records.")


if __name__ == '__main__':
    telecommunication_py()
