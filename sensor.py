

import json
import time
import random
from AWSIoTPythonSDK.MQTTLib import AWSIoTMQTTClient

# Endpoint
ENDPOINT = "aupk7fyv0xafi-ats.iot.us-east-1.amazonaws.com"
CLIENT_ID = "cold-storage-multi-sensor-1"

# Certificates
ROOT_CA = "/home/ec2-user/environment/AmazonRootCA1.pem"
PRIVATE_KEY = "/home/ec2-user/environment/private.key"
CERTIFICATE = "/home/ec2-user/environment/certificate.pem.crt"

TOPIC = "cold/storage/data"

# Create MQTT client
client = AWSIoTMQTTClient(CLIENT_ID)
client.configureEndpoint(ENDPOINT, 8883)
client.configureCredentials(ROOT_CA, PRIVATE_KEY, CERTIFICATE)

# Configurations
client.configureAutoReconnectBackoffTime(1, 32, 20)
client.configureOfflinePublishQueueing(-1)
client.configureDrainingFrequency(2)
client.configureConnectDisconnectTimeout(10)
client.configureMQTTOperationTimeout(5)


print("Connecting to IoT Core...")
client.connect()
print("Connected!")

while True:
    payload = {
        "device_id": "CS_MAIN",

        # 🌡️ Temperature sensor (°C)
        "temperature": round(random.uniform(-5, 12), 2),

        #  Humidity sensor (%)
        "humidity": round(random.uniform(60, 95), 2),

        #  Door sensor
        "door_status": "OPEN" if random.random() < 0.1 else "CLOSED",

        #  CO2 / Air Quality sensor (ppm)
        "co2_level": random.randint(300, 1000),

        #  Power consumption sensor (kWh)
        "power_usage": round(random.uniform(0.5, 5.0), 2),

        "timestamp": int(time.time())
    }

    client.publish(TOPIC, json.dumps(payload), 1)

    print("Sent:", payload)

    time.sleep(10)