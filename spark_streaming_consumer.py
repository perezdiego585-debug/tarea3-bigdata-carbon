# Consumidor Spark Streaming - Tarea 3 -Diego perez
# Lee los despachos de carbón del topic de Kafka y calcula cada minuto,
# por departamento: número de viajes, toneladas totales y ceniza promedio.
# Ejecutar:
# spark-submit 

from pyspark.sql import SparkSession
from pyspark.sql.functions import from_json, col, window
from pyspark.sql.types import StructType, StructField, IntegerType, FloatType, StringType, TimestampType

# Crear la sesión de Spark
spark = SparkSession.builder.appName("KafkaSparkStreaming_Carbon").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# Esquema de los datos que envía el productor
schema = StructType([
    StructField("camion_id", IntegerType()),
    StructField("departamento", StringType()),
    StructField("toneladas", FloatType()),
    StructField("ceniza", FloatType()),
    StructField("humedad", FloatType()),
    StructField("timestamp", TimestampType())
])

# Leer desde Kafka
df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "despachos_carbon") \
    .load()

# Convertir el JSON en columnas
datos = df.select(from_json(col("value").cast("string"), schema).alias("data")).select("data.*")

# Estadísticas por ventana de 1 minuto y por departamento
estadisticas = datos \
    .groupBy(window(col("timestamp"), "1 minute"), "departamento") \
    .agg({"camion_id": "count", "toneladas": "sum", "ceniza": "avg"})

# Mostrar los resultados en la consola
query = estadisticas \
    .writeStream \
    .outputMode("complete") \
    .format("console") \
    .option("truncate", False) \
    .start()

query.awaitTermination()
