import random
import sys
from datetime import datetime

import paho.mqtt.client as paho
import time

def on_publish(client, userdata, mid):
    print(f"Message published (mid: {mid})")

client = paho.Client()
client.on_publish = on_publish

if client.connect("localhost", 1883, 60) != 0:
    print("Couldn't connect to the mqtt broker")
    sys.exit(1)

client.loop_start()  # Start the loop in a separate thread

# Wait for the connection to be established
while not client.is_connected():
    time.sleep(1)

# Publish a message to "test_topic"
result = client.publish("test_topic", f"{500}**{random.randint(0, 10)}", 0)

# Wait for the message to be published
result.wait_for_publish()

client.loop_stop()  # Stop the loop

client.disconnect()

