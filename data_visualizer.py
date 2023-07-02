import matplotlib.pyplot as plt
from pymongo import MongoClient

# Conectando ao MongoDB
client = MongoClient('mongodb://localhost:27017')
db = client['sensors_data']
collection = db['cpu_percent']

# Consultando os dados do MongoDB
data = collection.find()

# Extraindo os dados necessários para visualização
x_values = []
y_values = []
for document in data:
    x_values.append(document['timestamp'])
    y_values.append(document['value'])

# Plotando o gráfico
plt.plot(x_values, y_values)
plt.xlabel('Timestamp')
plt.ylabel('Value')
plt.title(document['machine_id'])
plt.savefig('grafico.png')
plt.ion()
plt.show()
