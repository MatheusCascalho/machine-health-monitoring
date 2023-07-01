import time
import paho.mqtt.client as mqtt
from datetime import datetime
from bson import ObjectId
from bson.json_util import dumps
from pymongo import MongoClient
import json

QOS = 1
BROKER_PORT = 1883
BROKER_URI = "localhost"
BROKER_HOST = f"tcp://{BROKER_URI}"
BROKER_ADDRESS = f"{BROKER_HOST}:{BROKER_PORT}"
MONGO_DB_URL = "mongodb://localhost:27017"
MONGO_DB_NAME = "sensors_data"


def insert_document(collection, machine_id, timestamp_str, value):
    timestamp = datetime.strptime(timestamp_str, "%Y-%m-%dT%H:%M:%SZ")
    timestamp_timestamp = int(time.mktime(timestamp.timetuple()))

    doc = {
        "machine_id": machine_id,
        "timestamp": timestamp_timestamp,
        "value": value
    }

    try:
        collection.insert_one(doc)
    except Exception as e:
        print(f"Failed to insert doc: {e}")


def split_string(string, delim):
    return string.split(delim)


def on_message(client, userdata, message):
    payload = message.payload.decode("utf-8")
    data = dumps(payload)
    j = json.loads(data)

    topic = message.topic
    topic_parts = split_string(topic, '/')
    machine_id = topic_parts[2]
    sensor_id = topic_parts[3]

    timestamp = j["timestamp"]
    value = j["value"]

    client = MongoClient(MONGO_DB_URL)
    db = client[MONGO_DB_NAME]
    collection = db[sensor_id]

    insert_document(collection, machine_id, timestamp, value)


def main():
    client_id = "clientId"
    client = mqtt.Client(client_id=client_id)

    mongodb_client = MongoClient(MONGO_DB_URL)
    database = mongodb_client[MONGO_DB_NAME]

    callback = on_message
    callback.db = database

    client.on_message = callback

    try:
        client.connect(host=BROKER_URI, port=BROKER_PORT)
        client.subscribe("/sensors/#", QOS)
    except Exception as e:
        print(f"Error: {str(e)}")
        return

    try:
        while True:
            client.loop()
            time.sleep(1)
    except KeyboardInterrupt:
        pass
    finally:
        client.disconnect()

        mongodb_client.close()


if __name__ == "__main__":
    main()
