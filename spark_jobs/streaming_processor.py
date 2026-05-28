import os
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, from_json, sha2, concat, lit
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType
def main():
    print("[*] Initializing Apache Spark Session with Native Kafka Utilities...")
    
    # We explicitly declare the open-source Spark-SQL Kafka package matching our Confluent version
    # This automatically handles downloading the necessary underlying Java JAR libraries.
    spark = SparkSession.builder \
        .appName("PatientHealthMetricsProcessor") \
        .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true") \
        .master("local[*]") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # Define the strict data validation schema to parse the raw incoming JSON strings
    vitals_schema = StructType([
        StructField("device_id", StringType(), True),
        StructField("patient_id", StringType(), True),
        StructField("timestamp", StringType(), True),
        StructField("heart_rate", IntegerType(), True),
        StructField("blood_oxygen_pct", DoubleType(), True),
        StructField("systolic_bp", IntegerType(), True),
        StructField("diastolic_bp", IntegerType(), True),
        StructField("status_code", StringType(), True)
    ])

    print("[+] Connecting Spark Stream to Broker '127.0.0.1:9092'...")
    
    # Ingest raw message key/value bytes from Kafka with resilience options
    raw_kafka_stream = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", "127.0.0.1:9092") \
        .option("subscribe", "telemetry-patient-vitals") \
        .option("startingOffsets", "latest") \
        .option("failOnDataLoss", "false") \
        .load()

    # Convert the binary payload string back into standard validated columnar rows
    parsed_stream = raw_kafka_stream \
        .selectExpr("CAST(value AS STRING) as json_payload") \
        .select(from_json(col("json_payload"), vitals_schema).alias("data")) \
        .select("data.*")

    print("[*] Applying Cryptographic SHA-256 Masking on Patient Personal Identifiers...")
    
    # To mimic a secure healthcare environment, we mask the plain-text Patient UUID 
    # inside the stream by calculating its SHA-256 signature using a secure salt string
    SALT_VALUE = "clinical-security-token-2026"
    
    governed_stream = parsed_stream.withColumn(
        "patient_id_masked", 
        sha2(concat(col("patient_id"), lit(SALT_VALUE)), 256)
    ).drop("patient_id") # Immediately drop the cleartext ID from memory tracking

    # Map output storage destinations inside our project directory structure
    output_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "silver_vitals"))
    checkpoint_dir = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", "data", "checkpoints"))

    print(f"[+] Directing processed stream to Silver Parquet Warehouse: {output_dir}")

    # Start the continuous micro-batch streaming engine
    query = governed_stream.writeStream \
        .format("parquet") \
        .outputMode("append") \
        .option("path", output_dir) \
        .option("checkpointLocation", checkpoint_dir) \
        .start()

    print("[+] Silver Stream pipeline is now actively running in background.")
    print("[*] Listening for events... Press Ctrl+C to stop processor.")
    
    # Keep the execution thread active until manual interruption
    query.awaitTermination()

if __name__ == "__main__":
    main()