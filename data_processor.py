import paho.mqtt.client as mqtt
from pymongo import MongoClient
import threading
import json
from datetime import datetime, timedelta
import time

MONGO_DB_URL = "mongodb://localhost:27017"
MONGO_DB_NAME = "sensors_data"

# Lista para armazenar as threads criadas
threads = []
machines_listening = {}

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
        with threading.Lock():
            machines_listening[machine_id][sensor_id] = {
                'data_interval': data_interval
            }

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
    # Obtém o tópico e a mensagem recebida
    topico = msg.topic
    mensagem = msg.payload.decode("utf-8")
    if topico == '/sensor_monitors':
        data_rec = json.loads(mensagem)
        machine_id = data_rec['machine_id']
        #bloqueia threads que tentam modificar machines_listening ate que seja liberada
        with threading.Lock():
            if machine_id not in machines_listening:
                machines_listening[machine_id] = {}
                # Cria uma nova thread para processar a mensagem
                thread = threading.Thread(target=subscribe_process, args=(machine_id, mensagem))
                thread.start()
                # Adiciona a thread à lista de threads
                threads.append(thread)

    else:
        token = split_string(topico, '/')
        parse_msg = json.loads(mensagem)
        timestamp = parse_msg['timestamp']
        machine_id = int(token[2])
        sensor_id = token[3]
        value = parse_msg['value']
        with threading.Lock():
            if machine_id in machines_listening:
                insert_db(machine_id, sensor_id, timestamp, value)
                machines_listening[machine_id][sensor_id]['timestamp'] = timestamp

                current_time = datetime.now()
                #current_time = current_time.strftime("%Y-%m-%dT%H:%M:%S")
                sensor_time_iso = machines_listening[machine_id][sensor_id]['timestamp']
                sensor_time = datetime.strptime(sensor_time_iso, "%Y-%m-%dT%H:%M:%SZ")
                tempo_decorrido = current_time - sensor_time
                # print(f"tempo atual: {current_time}\n")
                # print(f"tempo sensor: {sensor_time}\n")
                # print(f"tempo decorrido: {tempo_decorrido}\n")

def monitora():
    client = MongoClient(MONGO_DB_URL)
    db = client[MONGO_DB_NAME]
    
    for machine_id, sensors in machines_listening.items():
        print(f"dicionario {machine_id} {sensors}\n")
        for sensor_id, sensor_data in sensors.items():
            print(f"sensores {sensor_id} {sensor_data}\n")
            collection = db[sensor_id]
            print(f"colecao {collection}\n")
            cursor = collection.find({
                'machine_id': machine_id,
            }).sort("timestamp", -1).limit(1)
            
            if cursor.count() > 0:
                result = cursor[0]
                last_timestamp = result['timestamp']
                current_time = datetime.now()
                sensor_time = datetime.strptime(last_timestamp, "%Y-%m-%dT%H:%M:%SZ")
                elapsed_time = current_time - sensor_time
                expected_interval = timedelta(milliseconds=sensor_data['data_interval'])
                
                if elapsed_time.total_seconds()/1000 > (10 * expected_interval.total_seconds()/1000):
                    alarm(machine_id, sensor_id)
                    print(f"Alarme gerado para a máquina {machine_id}, sensor {sensor_id}!")
                else:
                    print(f"Tempo decorrido para a máquina {machine_id}, sensor {sensor_id}: {elapsed_time}")
            else:
                print(f"Nenhum registro encontrado para a máquina {machine_id}, sensor {sensor_id}.\n")

def monitorar_banco():
    while True:
        monitora()
        time.sleep(1)  # Intervalo de verificação do banco de dado


# Cria uma nova thread para monitorar o banco de dados e gerar alarmes
monitor_thread = threading.Thread(target=monitorar_banco)
monitor_thread.daemon = True  # Define a thread como daemon para encerrar junto com o programa principal
monitor_thread.start()

# Configuração do cliente MQTT
client = mqtt.Client(client_id= "data_processor")

#funções de callback
client.on_connect = on_connect
client.on_message = on_message

print(f"\nMonitoradas: {machines_listening}\n")

client.connect("localhost", 1883, 120)

#loop para manter a conexão com o broker MQTT e processar as mensagens recebidas
client.loop_forever()

# Aguarda a finalização de todas as threads
for thread in threads:
    thread.join()

