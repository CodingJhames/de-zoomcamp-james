from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType, DoubleType

# Configuración validada para tu James-T-850 (Java 17 + Scala 2.13 + PySpark 4.1.1)
spark = SparkSession.builder \
    .appName("TaxiStreamingHomework") \
    .config("spark.jars.packages", "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

# Esquema para capturar todos los datos del examen de Octubre
schema = StructType([
    StructField("vendor_id", IntegerType()),
    StructField("pickup_datetime", StringType()),
    StructField("passenger_count", IntegerType()),
    StructField("trip_distance", DoubleType()),
    StructField("PULocationID", IntegerType()),
    StructField("DOLocationID", IntegerType()),
    StructField("tip_amount", DoubleType()),
    StructField("total_amount", DoubleType()),
])

# Lectura desde Redpanda (compatible con Kafka)
df_rides = spark.readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "rides") \
    .option("startingOffsets", "earliest") \
    .load() \
    .selectExpr("CAST(value AS STRING)") \
    .select(F.from_json(F.col("value"), schema).alias("data")) \
    .select("data.*") \
    .withColumn("pickup_timestamp", F.to_timestamp("pickup_datetime")) \
    .filter(F.col("vendor_id") == 1)  # <--- Esta línea es la que filtra la evidencia

# --- CONSULTAS PARA EL CUESTIONARIO ---

# Q3: Suma total de distancias
query3 = df_rides.agg(F.sum("trip_distance").alias("total_distance")) \
    .writeStream.outputMode("complete").format("console").start()

# Q4: Ventana de 5 min - Zona de recogida (PULocationID) más frecuente
query4 = df_rides.groupBy(F.window("pickup_timestamp", "5 minutes"), "PULocationID") \
    .count().orderBy(F.desc("count")) \
    .writeStream.outputMode("complete").format("console").start()

# Q6: Ventana de 5 min - Propina (tip_amount) más grande
query6 = df_rides.groupBy(F.window("pickup_timestamp", "5 minutes")) \
    .agg(F.max("tip_amount").alias("max_tip")).orderBy(F.desc("max_tip")) \
    .writeStream.outputMode("complete").format("console").start()

spark.streams.awaitAnyTermination()
