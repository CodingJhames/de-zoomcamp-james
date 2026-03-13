from kafka import KafkaConsumer
import json

# Configuración del Consumidor
consumer = KafkaConsumer(
    'rides',
    bootstrap_servers=['localhost:9092'],
    auto_offset_reset='earliest', # Empieza desde el primer mensaje disponible
    value_deserializer=lambda x: json.loads(x.decode('utf-8'))
)

print("Escuchando mensajes en el tópico 'rides'...")

for message in consumer:
    print(f"Recibido en el despacho: {message.value}")
