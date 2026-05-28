from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    print("[*] Initializing Spark Session for Gold Layer Aggregations...")
    
    # Spin up local Spark instance
    spark = SparkSession.builder \
        .appName("GoldLayerProcessing") \
        .master("local[*]") \
        .getOrCreate()
        
    spark.sparkContext.setLogLevel("ERROR")
    
    # Define absolute input and output paths
    silver_path = r"C:\Users\Windows\OneDrive\Documents\GitHub\Patient_Health_Metrics_Pipeline\data\silver_vitals"
    gold_patient_path = r"C:\Users\Windows\OneDrive\Documents\GitHub\Patient_Health_Metrics_Pipeline\data\gold_patient_risk_profiles"
    gold_trends_path = r"C:\Users\Windows\OneDrive\Documents\GitHub\Patient_Health_Metrics_Pipeline\data\gold_hourly_vitals_trend"
    
    try:
        # 1. Load data from the Silver Parquet Layer
        print("[+] Loading clean telemetry data from Silver layer...")
        silver_df = spark.read.parquet(silver_path)
        
        # 2. Compute Table 1: Patient Risk Profiles
        print("[+] Synthesizing Gold Table: Patient Risk Profiles...")
        patient_risk_profiles = silver_df.groupBy("patient_id_masked") \
            .agg(
                F.count("timestamp").alias("total_readings_logged"),
                F.round(F.avg("heart_rate"), 1).alias("avg_heart_rate"),
                F.round(F.avg("blood_oxygen_pct"), 1).alias("avg_spo2_pct"),
                F.max("heart_rate").alias("max_recorded_heart_rate"),
                F.min("blood_oxygen_pct").alias("min_recorded_spo2_pct"),
                F.count(F.when(F.col("status_code").contains("CRITICAL"), 1)).alias("critical_alert_count")
            ) \
            .withColumn("risk_tier", 
                F.when(F.col("critical_alert_count") >= 5, "HIGH RISK")
                .when((F.col("critical_alert_count") > 0) & (F.col("critical_alert_count") < 5), "MONITORING REQUIRED")
                .otherwise("STABLE")
            )
            
        # 3. Compute Table 2: Hourly Vitals Trend (Time Series)
        print("[+] Synthesizing Gold Table: Hourly Vital Trends...")
        # Cast timestamp string to actual timestamp, then truncate to the hour
        hourly_trends = silver_df \
            .withColumn("timestamp_parsed", F.to_timestamp("timestamp")) \
            .withColumn("window_hour", F.date_format("timestamp_parsed", "yyyy-MM-dd HH:00:00")) \
            .groupBy("window_hour") \
            .agg(
                F.count("timestamp").alias("total_ward_readings"),
                F.count(F.when(F.col("status_code") == "CRITICAL_HIGH", 1)).alias("tachycardia_events"),
                F.count(F.when(F.col("status_code") == "CRITICAL_LOW", 1)).alias("bradycardia_events"),
                F.round(F.avg("heart_rate"), 1).alias("ward_avg_hr")
            ) \
            .orderBy("window_hour")

        # 4. Write Results back to Disk as Clean Production Parquet files
        print("[+] Materializing Gold Tables to storage...")
        
        patient_risk_profiles.write \
            .mode("overwrite") \
            .parquet(gold_patient_path)
            
        hourly_trends.write \
            .mode("overwrite") \
            .parquet(gold_trends_path)
            
        # 5. Display a sample of what you just built for validation
        print("\n" + "="*60)
        print("          🏆 GOLD MATERIALIZATION SUCCESSFUL 🏆")
        print("="*60)
        print("👀 HIGH RISK PATIENTS IDENTIFIED:")
        patient_risk_profiles.filter(F.col("risk_tier") == "HIGH RISK") \
            .select("patient_id_masked", "total_readings_logged", "critical_alert_count", "risk_tier") \
            .show(5, truncate=False)
            
        print("-"*60)
        print("👀 WARD HOURLY TREND METRICS:")
        hourly_trends.show(5, truncate=False)
        print("="*60 + "\n")
        
    except Exception as e:
        print(f"[-] Critical error processing Gold layer transformations: {e}")
    finally:
        spark.stop()
        print("[+] Analytics Spark Engine shut down gracefully.")

if __name__ == "__main__":
    main()