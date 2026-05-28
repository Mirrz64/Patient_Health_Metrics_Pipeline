import json
import time
import random
import uuid
from datetime import datetime
from faker import Faker
from kafka import KafkaProducer

# Initialize Faker for realistic medical data generation
fake = Faker()

# Configuration constants
KAFKA_BOOTSTRAP_SERVERS = ['127.0.0.1:9092']
VITALS_TOPIC = 'telemetry-patient-vitals'
NUM_PATIENTS_POOL = 50  # Number of simulated patients in the ecosystem

def create_kafka_producer():
    """Initializes and returns a resilient Kafka Producer client."""
    try:
        return KafkaProducer(
            bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
            value_serializer=lambda v: json.dumps(v, default=str).encode('utf-8'),
            acks=1,       
            retries=3,
            linger_ms=0   
        )
    except Exception as e:
        print(f"[-] Error initializing Kafka Producer: {e}")
        exit(1)

def generate_patient_pool(num_patients):
    """Generates a static pool of patients to simulate a real clinical cohort."""
    patients = []
    genders = ['Male', 'Female', 'Other', 'Undisclosed']
    providers = ['Aetna', 'Blue Cross Blue Shield', 'Cigna', 'UnitedHealthcare', 'Humana']
    
    print(f"[+] Pre-generating {num_patients} secure patient profiles...")
    for _ in range(num_patients):
        patient_id = str(uuid.uuid4())
        profile = {
            "patient_id": patient_id,
            "full_name": fake.name(),
            "social_security_number": fake.ssn(),
            "date_of_birth": fake.date_of_birth(minimum_age=18, maximum_age=90).isoformat(),
            "gender": random.choice(genders),
            "primary_physician": f"Dr. {fake.last_name()}",
            "insurance_provider": random.choice(providers),
            "registration_timestamp": datetime.utcnow().isoformat() + "Z"
        }
        patients.append(profile)
    return patients

def generate_vitals_reading(patient_id):
    """Simulates real-time clinical telemetry streaming from a wearable IoT device."""
    condition_roll = random.random()
    
    if condition_roll > 0.95:  # 5% chance of simulating tachycardia/anomalous reading
        heart_rate = random.randint(125, 170)
        status_code = "CRITICAL_HIGH"
        systolic = random.randint(140, 160)
        diastolic = random.randint(90, 100)
        spo2 = round(random.uniform(88.0, 93.9), 1)
    elif condition_roll < 0.02: # 2% chance of simulating bradycardia
        heart_rate = random.randint(40, 50)
        status_code = "CRITICAL_LOW"
        systolic = random.randint(90, 100)
        diastolic = random.randint(55, 64)
        spo2 = round(random.uniform(94.0, 100.0), 1)
    else:                       # Normal resting ranges
        heart_rate = random.randint(60, 100)
        status_code = "NORMAL"
        systolic = random.randint(110, 129)
        diastolic = random.randint(70, 84)
        spo2 = round(random.uniform(95.0, 100.0), 1)

    return {
        "device_id": f"DEV-BIO-{patient_id[:8].upper()}",
        "patient_id": patient_id,
        "timestamp": datetime.utcnow().isoformat() + "Z",
        "heart_rate": heart_rate,
        "blood_oxygen_pct": spo2,
        "systolic_bp": systolic,
        "diastolic_bp": diastolic,
        "status_code": status_code
    }

def main():
    print("[*] Launching Patient Health Metrics Stream Engine...")
    producer = create_kafka_producer()
    patient_pool = generate_patient_pool(NUM_PATIENTS_POOL)
    
    print(f"[+] Ingestion engine active. Streaming to topic: '{VITALS_TOPIC}'...")
    print("[*] Press Ctrl+C to terminate stream simulation.\n" + "-"*50)
    
    try:
        while True:
            # Pick a random patient from our pre-generated master cohort
            active_patient = random.choice(patient_pool)
            vitals_payload = generate_vitals_reading(active_patient['patient_id'])
            
            # Send payload asynchronously to Kafka
            producer.send(VITALS_TOPIC, value=vitals_payload)
            
            # Explicitly force the network packet out of the Python buffer down the socket
            producer.flush()
            
            print(f"[STREAM] Sent vitals for Patient {vitals_payload['patient_id'][:8]} -> HR: {vitals_payload['heart_rate']} bpm | SpO2: {vitals_payload['blood_oxygen_pct']}% | Status: {vitals_payload['status_code']}")
            
            # Throttle stream to simulate a real continuous clock cycle (1 reading per second)
            time.sleep(1.0)
            
    except KeyboardInterrupt:
        print("\n[*] Gracefully shutting down streaming engine...")
    finally:
        producer.close()
        print("[+] Kafka producer instance closed successfully.")

if __name__ == "__main__":
    main()