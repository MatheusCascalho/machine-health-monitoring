import asyncio
from datetime import datetime
import json
import psutil
from paho.mqtt.client import Client
import uuid

QOS = 1
BROKER_PORT = 1883
BROKER_URI = "localhost"
BROKER_HOST = f"tcp://{BROKER_URI}"
BROKER_ADDRESS = f"{BROKER_HOST}:{BROKER_PORT}"


# Callback functions to deal with MQTT events
def on_publish(client: Client, userdata, mid):
    msg = userdata.get("msg")
    print(f"Mensagem publicada: {msg}")


def on_connect(client, userdata, flags, rc):
    print("Conectado: " + str(rc))
    client.subscribe(userdata)


def on_disconnect(client, userdata, rc):
    if rc != 0:
        print("Desconexão inesperada")


# Setting MQTT client
def set_client() -> Client:
    client = Client()
    client.on_publish = on_publish
    client.on_connect = on_connect
    client.on_disconnect = on_disconnect
    client.connect(host=BROKER_URI, port=BROKER_PORT)
    return client


def get_time():
    return str(datetime.now().isoformat())


async def publish_cpu(client: Client):
    cpu = psutil.cpu_percent()
    timestamp = get_time()
    data = {
        "timestamp": timestamp,
        "value": cpu
    }
    data = json.dumps(data)
    machine_id = get_machine_id()
    topic = f"/sensors/{machine_id}/cpu_percent"
    client.publish(topic=topic, payload=data)


async def publish_memory(client: Client):
    memory = psutil.virtual_memory()[2]
    timestamp = get_time()
    data = {
        "timestamp": timestamp,
        "value": memory
    }
    data = json.dumps(data)
    machine_id = get_machine_id()
    topic = f"/sensors/{machine_id}/memory_percent"
    client.publish(topic=topic, payload=data)


async def read_sensors(client: Client):
    tasks = [
        asyncio.create_task(publish_cpu(client)),
        asyncio.create_task(publish_memory(client))
    ]
    await asyncio.gather(*tasks)


async def sensor_loop(client: Client, period):
    while True:
        asyncio.run(read_sensors(client))
        await asyncio.sleep(period)


async def publish_identifier(client: Client, period):
    machine_id = get_machine_id()
    data = {
        "machine_id": machine_id,
        "sensors": [
            {
                "sensor_id": "memory_percent",
                "data_type": "float",
                "data_interval": period
            },
            {
                "sensor_id": "cpu_percent",
                "data_type": "float",
                "data_interval": period
            }
        ]
    }
    data = json.dumps(data)
    topic = "/sensor_monitors"
    client.publish(topic=topic, payload=data)


async def identifier_loop(client: Client, period: float):
    while True:
        asyncio.run(publish_identifier(client=client, period=period))
        await asyncio.sleep(period)


def get_machine_id():
    machine_id = uuid.getnode()
    return machine_id


def main():
    sensor_period = 1
    identifier_period = 5
    client = set_client()
    sensor_loop(client=client, period=sensor_period)
    identifier_loop(client=client, period=identifier_period)


if __name__ == "__main__":
    main()



