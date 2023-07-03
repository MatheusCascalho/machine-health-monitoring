import paho.mqtt.client as mqtt
from pymongo import MongoClient
import threading
import json

MONGO_DB_URL = "mongodb://localhost:27017"
MONGO_DB_NAME = "sensors_data"

# Lista para armazenar as threads criadas
threads = []
machines_listening = []

machine_id = 0

def split_string(string, delimiter):
    parts = string.split(delimiter)
    return parts

def subscribe_process(machine_id, mensagem):
    print(f"Mensagem recebida no tópico: {mensagem}")
    parser_msg = json.loads(mensagem)
    sensors = parser_msg['sensors']
    for sensor in sensors:
        sensor_id = sensor['sensor_id']
        data_interval = sensor['data_interval']
        client.subscribe(f"/sensors/{machine_id}/{sensor_id}", qos=1)

def insert_db(machine_id, sensor_id, timestamp, value):
    client = MongoClient(MONGO_DB_URL)
    db = client[MONGO_DB_NAME]
    collection = db[sensor_id]
    doc = {
        "machine_id": machine_id,
        "timestamp": timestamp,
        "value": value
    }
    try:
        collection.insert_one(doc)
    except Exception as e:
        print(f"Failed to insert doc: {e}")

def alarm(machine_id, sensor_id):
    client = MongoClient(MONGO_DB_URL)
    db = client[MONGO_DB_NAME]
    collection = db['alarms']
    doc = {
        "machine_id": machine_id,
        "sensor_id": sensor_id,
        "description": "Sensor inativo por dez períodos de tempo previstos" 
    }
    try:
        collection.insert_one(doc)
    except Exception as e:
        print(f"Failed to insert doc: {e}")

# Callback para quando o cliente se conecta ao broker MQTT
def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker MQTT. Código de resultado: " + str(rc))
    # Inscreva-se no tópico desejado quando estiver conectado
    client.subscribe("/sensor_monitors")
    #client.subscribe("/sensors/#")

# Callback para quando uma nova mensagem é recebida em um tópico inscrito
def on_message(client, userdata, msg):
    # Exiba o tópico e o conteúdo da mensagem recebida
    print("Tópico: " + msg.topic)
    print("Conteúdo: " + msg.payload.decode())
    count = threading.active_count()
    print(f"Total de threads vivas: {count}")
    # Obtém o tópico e a mensagem recebida
    topico = msg.topic
    mensagem = msg.payload.decode("utf-8")
    if topico == '/sensor_monitors':
        data_rec = json.loads(mensagem)
        global machine_id 
        machine_id = data_rec['machine_id']
        if machine_id not in machines_listening:
            machines_listening.append(machine_id)
            print(f"ID maquina: {machine_id}\n")
            # Cria uma nova thread para processar a mensagem
            thread = threading.Thread(target=subscribe_process, args=(machine_id, mensagem))
            thread.start()
            # Adiciona a thread à lista de threads
            threads.append(thread)

    else:
        token = split_string(topico, '/')
        #print(f"token: {token}\n")
        parse_msg = json.loads(mensagem)
        timestamp = parse_msg['timestamp']
        value = parse_msg['value']
        insert_db(machine_id, token[3], timestamp, value)
        

# Configuração do cliente MQTT
client = mqtt.Client(client_id= "data_processor")

#funções de callback
client.on_connect = on_connect
client.on_message = on_message

client.connect("localhost", 1883, 120)

#loop para manter a conexão com o broker MQTT e processar as mensagens recebidas
client.loop_forever()

# Aguarda a finalização de todas as threads
for thread in threads:
    thread.join()
