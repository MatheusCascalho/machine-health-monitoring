import paho.mqtt.client as mqtt
from pymongo import MongoClient
import threading
import json

# Lista para armazenar as threads criadas
threads = []
machines_listening = []

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
        client.subscribe(f"/sensors/{machine_id}/{sensor_id}")

def insert_db(machine_id, sensor_id, timestemp, value):
    print('insert db \n')


def data_analize(topico, mensagem):
    token = split_string(topico, '/')
    print(f"{token[2]}:\n")
    parse_msg = json.loads(mensagem)

# Callback para quando o cliente se conecta ao broker MQTT
def on_connect(client, userdata, flags, rc):
    print("Conectado ao broker MQTT. Código de resultado: " + str(rc))
    # Inscreva-se no tópico desejado quando estiver conectado
    client.subscribe("/sensor_monitors")

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
    
        if machine_id not in machines_listening:
            machines_listening.append(machine_id)
            print(f"ID maquina: {machine_id}\n")
            # Cria uma nova thread para processar a mensagem
            thread = threading.Thread(target=subscribe_process, args=(machine_id, mensagem))
            thread.start()
            # Adiciona a thread à lista de threads
            threads.append(thread)

    else:
        print('sensors\n')
        

# Configuração do cliente MQTT
client = mqtt.Client()

# Defina as funções de callback
client.on_connect = on_connect
client.on_message = on_message

# Conecte-se ao broker MQTT
client.connect("localhost", 1883, 60)

# Inicie o loop para manter a conexão com o broker MQTT e processar as mensagens recebidas
client.loop_forever()

# Aguarda a finalização de todas as threads
for thread in threads:
    thread.join()
