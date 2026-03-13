import json
import time
from kafka import KafkaProducer

# Configuración del Productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

print("Iniciando envío de datos...")

# Simulamos el envío de 10 mensajes
for i in range(10):
    data = {'taxi_id': i, 'passenger_count': i + 1, 'status': 'ongoing'}
    producer.send('rides', value=data)
    print(f"Enviado: {data}")
    time.sleep(1) # Esperamos 1 segundo entre mensajes

producer.flush()

