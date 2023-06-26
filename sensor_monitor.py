import asyncio
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import json
import multiprocessing
import psutil
from paho.mqtt.client import Client
import time
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


def publish_cpu(client: Client):
    cpu = psutil.cpu_percent()
    timestamp = get_time()
    time.sleep(1)
    data = {
        "timestamp": timestamp,
        "value": cpu
    }
    data = json.dumps(data)
    machine_id = get_machine_id()
    topic = f"/sensors/{machine_id}/cpu_percent"
    print(f"Publicando a mensagem de CPU: {data}")
    client.publish(topic=topic, payload=data, qos=QOS)
    print("Mensagem Publicada!!")
    return "OK - CPU"


def publish_memory(client: Client):
    memory = psutil.virtual_memory()[2]
    timestamp = get_time()
    data = {
        "timestamp": timestamp,
        "value": memory
    }
    data = json.dumps(data)
    machine_id = get_machine_id()
    topic = f"/sensors/{machine_id}/memory_percent"
    print(f"Publicando a mensagem de RAM: {data}")
    client.publish(topic=topic, payload=data, qos=QOS)
    print("Mensagem Publicada!!")
    return "OK - RAM"


async def loop(publisher: callable, client: Client, period):
    while True:
        publisher(client)
        await asyncio.sleep(period)


def loop_cpu(client: Client, period):
    while True:
        publish_cpu(client)
        time.sleep(period)
        # await asyncio.sleep(period)


def loop_ram(client: Client, period):
    while True:
        publish_memory(client)
        time.sleep(period)
        # await asyncio.sleep(period)


async def sensor_loop(client: Client, period):
    process1 = multiprocessing.Process(target=loop_ram, args=(client, period))
    process2 = multiprocessing.Process(target=loop_cpu, args=(client, period))

    process1.start()
    process2.start()

    process1.join()
    process2.join()
    await asyncio.sleep(float('inf'))


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
    print(f"Publicando a mensagem: {data}")
    client.publish(topic=topic, payload=data, qos=QOS)
    print("mensagem publicada!")


async def identifier_loop(client: Client, period: float):
    while True:
        await publish_identifier(client=client, period=period)
        await asyncio.sleep(period)


def get_machine_id():
    machine_id = uuid.getnode()
    return machine_id


def main():
    sensor_period = .5
    identifier_period = .5
    client = set_client()
    asyncio.run(sensor_loop(client=client, period=sensor_period))
    # asyncio.run(identifier_loop(client=client, period=identifier_period))


if __name__ == "__main__":
    main()



