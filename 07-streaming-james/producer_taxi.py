import pandas as pd
from kafka import KafkaProducer
import json
import time

# 1. Configurar el productor
producer = KafkaProducer(
    bootstrap_servers=['localhost:9092'],
    value_serializer=lambda v: json.dumps(v).encode('utf-8')
)

# 2. Leer los datos de Octubre
print("Cargando datos de viajes...")
df = pd.read_parquet('yellow_tripdata_2025-10.parquet').head(7000)

print(f"Iniciando transmisión de {len(df)} viajes a Redpanda...")

count = 0  # Inicializamos el contador

for row in df.itertuples():
    data = {
        'vendor_id': row.VendorID,
        'pickup_datetime': str(row.tpep_pickup_datetime),
        'dropoff_datetime': str(row.tpep_dropoff_datetime),
        'passenger_count': row.passenger_count,
        'trip_distance': row.trip_distance,
        'PULocationID': row.PULocationID,
        'DOLocationID': row.DOLocationID,
        'tip_amount': row.tip_amount,
        'total_amount': row.total_amount
    }
    
    producer.send('rides', value=data)
    
    count += 1
    # Reporte de avance: solo imprime cada 100,000 registros
    if count % 100000 == 0:
        print(f"Progreso: {count} / {len(df)} viajes enviados...")

# 3. Sellar el envío
print("Vaciando el buffer de red (flush)...")
producer.flush()
print("¡Transmisión completada con éxito!")
