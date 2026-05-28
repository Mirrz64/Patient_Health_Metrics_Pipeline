from pyspark.sql import SparkSession

# Initialize a quick local Spark session
spark = SparkSession.builder \
    .appName("SilverLayerAudit") \
    .master("local[*]") \
    .getOrCreate()

# Suppress noisy logs so you only see your report
spark.sparkContext.setLogLevel("ERROR")

path = r"C:\Users\Windows\OneDrive\Documents\GitHub\Patient_Health_Metrics_Pipeline\data\silver_vitals"

try:
    # Read the data using Spark's native parquet reader
    df = spark.read.parquet(path)
    
    print("\n" + "="*50)
    print("       🎉 PIPELINE VALIDATION REPORT 🎉")
    print("="*50)
    print(f"[+] Total Records Saved to Silver Layer : {df.count()}")
    print(f"[+] Unique Patient Cohorts Monitored   : {df.select('patient_id_masked').distinct().count()}")
    print(f"[+] Critical Anomalies Logged         : {df.filter(df.status_code.contains('CRITICAL')).count()}")
    print("-"*50)
    print("👀 SAMPLE PARQUET SNAPSHOT (SPARK DATAFRAME):")
    df.select('timestamp', 'heart_rate', 'blood_oxygen_pct', 'status_code', 'patient_id_masked').show(5, truncate=False)
    print("="*50 + "\n")
    
except Exception as e:
    print(f"[-] Error reading Parquet directory with Spark: {e}")

finally:
    spark.stop()