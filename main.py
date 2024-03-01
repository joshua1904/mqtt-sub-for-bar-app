import sys
from sql import insert
import paho.mqtt.client as paho
import logging
logger = logging.getLogger(__name__)
logger.error("Started MQTT subscriber")
def message_handling(client, userdata, msg):
    logger.info(f"{msg.topic}: {msg.payload.decode()}")
    insert(msg.payload.decode())


client = paho.Client()
client.on_message = message_handling
#JUZ IP 192.168.178.77
if client.connect("host.docker.internal", 1883, 60) != 0:
    logger.info("Couldn't connect to the mqtt broker")
    sys.exit(1)

client.subscribe("test_topic")

try:
    logger.info("Press CTRL+C to exit...")
    client.loop_forever()
except Exception as e:
    logger.info("Caught an Exception, something went wrong...", e)
finally:
    logger.info("Disconnecting from the MQTT broker")
    client.disconnect()