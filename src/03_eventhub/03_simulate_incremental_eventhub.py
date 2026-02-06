# Databricks notebook source
# MAGIC %md
# MAGIC # SEND EVENTS DATA TO EVENTHUB.

# COMMAND ----------

pip install azure-eventhub

# COMMAND ----------

# DBTITLE 1,Working
# Imports
import json
import time
import random
import datetime
from azure.eventhub import EventHubProducerClient, EventData

# Secrets
eventhub_key = dbutils.secrets.get(scope="kv-accesskeys-gdc", key="evh-gdctraining-read-write")

# Build connection key
conn_str = "Endpoint=sb://evhns-gdctraining.servicebus.windows.net/;SharedAccessKeyName=SharedAccessKeyToSendAndListen;SharedAccessKey=ldeCpffK7Btui3LPg9wLF7uVL692Ehj1M+AEhJ3Xwgk=;EntityPath=evh-gdctraining-leon"

# Create event producer from connection key
producer = EventHubProducerClient.from_connection_string(conn_str=conn_str)

print("Sending events every 2 seconds...")

# Send events to event hub
with producer:
    for i in range(50): # run for n events
        payload = {
            "timestamp": datetime.datetime.now(datetime.UTC).isoformat(),
            "sequence": i,
            "deviceId": f"device-{random.randint(1,5)}",
            "temperatureC": round(random.uniform(20, 35), 2),
            "status": random.choice(["OK", "WARN", "ALERT"])
        }

        event = EventData(json.dumps(payload))
        producer.send_batch([event])

        print(f"Sent event #{i}: {payload}")

        time.sleep(2)  # Send every 2 seconds

