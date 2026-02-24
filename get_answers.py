import duckdb

# 1. Conectarse a la base de datos que dlt creó
# La ruta la sacamos de tu mensaje: /home/ubuntu/de-zoomcamp-james/ny_taxi_pipeline.duckdb
conn = duckdb.connect("ny_taxi_pipeline.duckdb")

# 2. Apuntar al dataset correcto
conn.sql("SET search_path = 'ny_taxi_data'")

print("--- RESPUESTAS TAREA 2026 ---")

# Pregunta 1: Rango de fechas
print("\n1. Rango de fechas (Start y End):")
res1 = conn.sql("SELECT MIN(trip_pickup_date_time), MAX(trip_pickup_date_time) FROM rides").df()
print(res1)

# Pregunta 2: Proporción de pagos con tarjeta (Buscando el texto 'Credit')
print("\n2. Proporción de pagos con tarjeta de crédito:")
query2 = """
SELECT 
    (COUNT(CASE WHEN payment_type = 'Credit' THEN 1 END) * 1.0 / COUNT(*)) as proportion 
FROM rides
"""
print(conn.sql(query2).show()) # Usamos .show() para ser más ligeros de RAM

# Pregunta 3: Total de propinas
print("\n3. Suma total de propinas (Tips):")
print(conn.sql("SELECT SUM(tip_amt) FROM rides").df())
