import json
import os
import random
import uuid
import time
from datetime import datetime, timedelta
from kafka import KafkaProducer
from kafka.errors import KafkaError

# ---------------------------------------------------------------
# Connection string is read from an environment variable instead of being
# hardcoded in source. Set it before running, e.g.:
#   export EVENTHUB_CONNECTION_STRING="Endpoint=sb://...;SharedAccessKey=..."
# Never commit real connection strings to git even as placeholders that
# look "obviously fake" -- keep the pattern of reading from the
# environment so it's a habit by the time real credentials are involved.
# ---------------------------------------------------------------
EVENTHUBS_NAMESPACE = os.environ.get("EVENTHUB_NAMESPACE", "<<NAMESPACE_HOSTNAME>>")
EVENT_HUB_NAME = os.environ.get("EVENTHUB_NAME", "<<EVENT_HUB_NAME>>")
CONNECTION_STRING = os.environ.get("EVENTHUB_CONNECTION_STRING")

if not CONNECTION_STRING:
    raise RuntimeError(
        "EVENTHUB_CONNECTION_STRING is not set. Export it as an environment "
        "variable before running this script."
    )

producer = KafkaProducer(
    bootstrap_servers=[f"{EVENTHUBS_NAMESPACE}:9093"],
    security_protocol="SASL_SSL",
    sasl_mechanism="PLAIN",
    sasl_plain_username="$ConnectionString",
    sasl_plain_password=CONNECTION_STRING,
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# Departments in hospital
departments = ["Emergency", "Surgery", "ICU", "Pediatrics", "Maternity", "Oncology", "Cardiology"]

# Gender categories
genders = ["Male", "Female"]

# Keep a small rolling pool of recently generated patient IDs so some
# events reuse an existing patient_id (simulating re-admission / follow-up
# visits). Without this, every event has a brand-new UUID and the SCD2
# change-detection logic in the gold layer never actually gets exercised.
_recent_patient_ids = []
_MAX_RECENT = 50
_REUSE_PROBABILITY = 0.15


def get_patient_id():
    if _recent_patient_ids and random.random() < _REUSE_PROBABILITY:
        return random.choice(_recent_patient_ids)
    new_id = str(uuid.uuid4())
    _recent_patient_ids.append(new_id)
    if len(_recent_patient_ids) > _MAX_RECENT:
        _recent_patient_ids.pop(0)
    return new_id


# Helper function to introduce dirty data
def inject_dirty_data(record):
    # 5% chance to have invalid age
    if random.random() < 0.05:
        record["age"] = random.randint(101, 150)

    # 5% chance to have future admission timestamp
    if random.random() < 0.05:
        record["admission_time"] = (datetime.utcnow() + timedelta(hours=random.randint(1, 72))).isoformat()

    return record


def generate_patient_event():
    admission_time = datetime.utcnow() - timedelta(hours=random.randint(0, 72))
    discharge_time = admission_time + timedelta(hours=random.randint(1, 72))

    event = {
        "patient_id": get_patient_id(),
        "gender": random.choice(genders),
        "age": random.randint(1, 100),
        "department": random.choice(departments),
        "admission_time": admission_time.isoformat(),
        "discharge_time": discharge_time.isoformat(),
        "bed_id": random.randint(1, 500),
        "hospital_id": random.randint(1, 7)  # Assuming 7 hospitals in network
    }

    return inject_dirty_data(event)


if __name__ == "__main__":
    try:
        while True:
            event = generate_patient_event()
            try:
                producer.send(EVENT_HUB_NAME, event).get(timeout=10)
                print(f"Sent to Event Hub: {event}")
            except KafkaError as e:
                print(f"Failed to send event, will retry next loop: {e}")
            time.sleep(1)
    except KeyboardInterrupt:
        print("\nShutting down simulator...")
    finally:
        producer.flush()
        producer.close()
