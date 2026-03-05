import pyspark
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

def main():
    # 1. Iniciar la sesión de Spark
    # Usamos local[*] para aprovechar todos los núcleos de nuestra instancia c7i-flex.large
    spark = SparkSession.builder \
        .master("local[*]") \
        .appName('DE-Zoomcamp-Homework-W5') \
        .getOrCreate()

    print(f"Spark Version: {spark.version}")

    # 2. Cargar datos de Yellow Taxi Noviembre 2025 (Parquet)
    # Nota: Parquet ya incluye el esquema, por lo que no es necesario definirlo manualmente.
    input_path = 'yellow_tripdata_2025-11.parquet'
    df = spark.read.parquet(input_path)

    # 3. Reparticionar los datos a 12 particiones (Pregunta 2)
    # Esto genera archivos de aproximadamente 25MB cada uno, optimizando el procesamiento paralelo.
    df_repartitioned = df.repartition(12)
    output_path = 'data/pq/yellow/2025/11/'
    df_repartitioned.write.parquet(output_path, mode='overwrite')
    print(f"Datos reparticionados y guardados en: {output_path}")

    # 4. Pregunta 3: ¿Cuántos viajes comenzaron el 15 de Noviembre?
    trips_nov_15 = df.filter(F.to_date(df.tpep_pickup_datetime) == '2025-11-15').count()
    print(f"Pregunta 3 - Viajes el 15 de Noviembre: {trips_nov_15}")

    # 5. Pregunta 4: El viaje más largo del dataset (en horas)
    # Calculamos la diferencia entre dropoff y pickup en segundos y convertimos a horas.
    df_duration = df.withColumn('duration_hours', 
        (F.unix_timestamp('tpep_dropoff_datetime') - F.unix_timestamp('tpep_pickup_datetime')) / 3600
    )
    max_duration = df_duration.select(F.max('duration_hours')).collect()[0][0]
    print(f"Pregunta 4 - Viaje más largo (en horas): {max_duration:.2f}")

    # 6. Pregunta 6: Zona con el menor número de recogidas (Pickups)
    # Primero cargamos el catálogo de zonas
    df_zones = spark.read \
        .option("header", "true") \
        .csv('taxi_zone_lookup.csv')

    # Realizamos un Join para obtener los nombres de las zonas basado en PULocationID
    df_joined = df.join(df_zones, df.PULocationID == df_zones.LocationID)

    # Agrupamos por Zona y ordenamos de forma ascendente
    least_pickup_zone = df_joined.groupBy('Zone') \
        .count() \
        .orderBy('count', ascending=True)

    print("Pregunta 6 - Zona con menos recogidas:")
    least_pickup_zone.show(1)

    # Mantener el Spark UI vivo por un momento si es necesario (opcional)
    # input("Presiona Enter para cerrar la sesión de Spark...")

if __name__ == "__main__":
    main()