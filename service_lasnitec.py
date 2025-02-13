import paho.mqtt.client as mqtt
import requests
import json
import urllib3

# Disable SSL warning
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Define the MQTT broker details
broker_host = "36.92.168.180"
broker_port = 19383
topics = [
    "lansitec/pub/860137071625429",
    "lansitec/pub/860137071526460"
]

# Antares Configuration
ANTARES_BASE_URL = "https://platform.antares.id:8443"
ANTARES_PATH = f"/~/antares-cse/antares-id/Tracker_Tanto"

# Initialize message counter
message_counter = 0

def send_to_antares(imei, payload):
    device_path = f"{ANTARES_PATH}/Lansitec_CAT1_{imei}"
    url = f"{ANTARES_BASE_URL}{device_path}"
    
    headers = {
        'X-M2M-Origin': '5aaf96c4cba802b1:e5c2804b39d4bad8',
        'Content-Type': 'application/json;ty=4',
        'Accept': 'application/json'
    }
    
    data = {
        "m2m:cin": {
            "con": payload
        }
    }
    
    try:
        response = requests.post(url, headers=headers, json=data, verify=False)
        if response.status_code == 201:
            print(f"Successfully sent data to Antares for IMEI: {imei}")
        else:
            print(f"Failed to send data. Status code: {response.status_code}")
            print(f"Response: {response.text}")
    except Exception as e:
        print(f"Error sending data to Antares: {str(e)}")

# Define the callback function for when a message is received
def on_message(client, userdata, message):
    global message_counter
    message_counter += 1
    # Extract IMEI from received message topic
    msg_imei = message.topic.split('/')[-1]
    payload = message.payload.decode()
    print(f"Message #{message_counter} - IMEI: {msg_imei}")
    print(f"Received: {payload} from topic: {message.topic}")
    
    # Send the received payload to Antares
    send_to_antares(msg_imei, payload)

# Create an MQTT client instance
client = mqtt.Client(client_id="MQTT_Subscriber", protocol=mqtt.MQTTv5)

# Assign the callback function
client.on_message = on_message

# Connect to the MQTT broker
client.connect(broker_host, broker_port)

# Subscribe to all topics
for topic in topics:
    client.subscribe(topic)
    print(f"Subscribed to topic: {topic}")

# Start the network loop to process received messages
try:
    print("Waiting for messages... (Press Ctrl+C to stop)")
    client.loop_forever()
except KeyboardInterrupt:
    print(f"\nSubscriber stopped by user")
    print(f"Total messages received: {message_counter}")
finally:
    client.disconnect()
