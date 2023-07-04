import matplotlib.pyplot as plt
from pymongo import MongoClient

# Conectando ao MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['sensors_data']

# Obtendo a lista de coleções (sensores) disponíveis no banco de dados
collections = db.list_collection_names()

# Imprimindo as coleções disponíveis
print("Coleções disponíveis:")
for index, collection_name in enumerate(collections):
    print(f"{index + 1}. {collection_name}")

# Solicitando ao usuário que selecione uma coleção (sensor)
selected_index = int(input("Selecione o número correspondente à coleção desejada: "))
selected_collection = collections[selected_index - 1]

# Verificando se a coleção selecionada é a de média móvel
if selected_collection == 'media_movel':
    # Obtendo a lista de sensores disponíveis na coleção de média móvel
    media_movel_collection = db[selected_collection]
    media_movel_sensores = media_movel_collection.distinct("sensor_id")

    # Imprimindo os sensores disponíveis na coleção de média móvel
    print("Sensores disponíveis:")
    for index, sensor_id in enumerate(media_movel_sensores):
        print(f"{index + 1}. {sensor_id}")

    # Solicitando ao usuário que selecione um sensor
    selected_sensor_index = int(input("Selecione o número correspondente ao sensor desejado: "))
    selected_sensor = media_movel_sensores[selected_sensor_index - 1]

    # Obtendo a lista de Machine IDs disponíveis para o sensor selecionado
    media_movel_machine_ids = media_movel_collection.distinct("machine_id")

    # Imprimindo os Machine IDs disponíveis para o sensor selecionado
    print("Machine IDs disponíveis:")
    for index, machine_id in enumerate(media_movel_machine_ids):
        print(f"{index + 1}. {machine_id}")

    # Solicitando ao usuário que selecione um Machine ID
    selected_machine_id_index = int(input("Selecione o número correspondente ao Machine ID desejado: "))
    selected_machine_id = media_movel_machine_ids[selected_machine_id_index - 1]

    # Consultando os dados da média móvel para o sensor e Machine ID selecionados
    media_movel_data = media_movel_collection.find({"sensor_id": selected_sensor, "machine_id": selected_machine_id})

    # Extraindo os dados necessários para visualização da média móvel
    media_movel_x_values = []
    media_movel_y_values = []
    for document in media_movel_data:
        media_movel_x_values.append(document['timestamp'])
        media_movel_y_values.append(document['value'])

    # Plotando o gráfico da média móvel do sensor e Machine ID selecionados
    plt.plot(media_movel_x_values, media_movel_y_values, label='Média Móvel', color='red')
else:
    # Obtendo os registros da coleção selecionada
    collection = db[selected_collection]
    data = collection.find()

    # Obtendo a lista de Machine IDs disponíveis na coleção selecionada
    machine_ids = collection.distinct("machine_id")

    # Imprimindo os Machine IDs disponíveis na coleção selecionada
    print("Machine IDs disponíveis:")
    for index, machine_id in enumerate(machine_ids):
        print(f"{index + 1}. {machine_id}")

    # Solicitando ao usuário que selecione um Machine ID
    selected_machine_id_index = int(input("Selecione o número correspondente ao Machine ID desejado: "))
    selected_machine_id = machine_ids[selected_machine_id_index - 1]

    # Extraindo os dados necessários para visualização do sensor e Machine ID selecionados
    sensor_x_values = []
    sensor_y_values = []
    for document in data:
        if document['machine_id'] == selected_machine_id:
            sensor_x_values.append(document['timestamp'])
            sensor_y_values.append(document['value'])

    # Plotando o gráfico do sensor e Machine ID selecionados
    plt.plot(sensor_x_values, sensor_y_values, label='Sensor', color='blue')

plt.xlabel('Timestamp')
plt.ylabel('Value')
plt.title(f"Coleção: {selected_collection} Machine ID: {selected_machine_id}")
plt.legend()  # Mostra a legenda dos sensores e da média móvel
plt.savefig('grafico'+selected_collection+' ' + str(machine_id) +'.png')
plt.show()
