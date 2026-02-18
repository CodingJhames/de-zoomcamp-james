from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .master("local[*]") \
    .appName('tarea5') \
    .getOrCreate()

# Leer Parquet directamente
df = spark.read.parquet('fhvhv_tripdata_2019-02.parquet')

# Q3: Viajes el 15 de Febrero
print("\n--- RESULTADO PREGUNTA 3 ---")
print(f"Viajes el 15 de Feb: {df.filter(F.to_date(df.pickup_datetime) == '2019-02-15').count()}")

# Q4: Viaje más largo (en segundos)
print("\n--- RESULTADO PREGUNTA 4 ---")
df.withColumn('duration', (F.unix_timestamp('dropoff_datetime') - F.unix_timestamp('pickup_datetime'))) \
    .select(F.max('duration')).show()

# Q6: Zona de recogida más frecuente
zones = spark.read.option("header", "true").option("inferSchema", "true").csv('taxi+_zone_lookup.csv')
df_join = df.join(zones, df.PULocationID == zones.LocationID)
print("\n--- RESULTADO PREGUNTA 6 ---")
df_join.groupBy('Zone').count().orderBy('count', ascending=False).show(1)