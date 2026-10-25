# -*- coding: utf-8 -*-
"""
Consumidor Spark Structured Streaming - Tarea 3 (Big Data UNAD)
Lee los despachos de carbón del topic de Kafka 'despachos_carbon' y calcula,
en ventanas de 1 minuto y por departamento:
  - número de viajes y toneladas despachadas
  - promedios de poder calorífico, humedad, cenizas y azufre
  - viajes fuera de especificación (cenizas > 15 % o azufre > 1,2 %)
Además muestra, evento por evento, las alertas de camiones fuera de especificación.

Ejecución (ver README):
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (DoubleType, LongType, StringType, StructField, StructType)

CENIZA_MAX = 15.0   # % - límite de especificación de ejemplo
AZUFRE_MAX = 1.2    # % - límite de especificación de ejemplo

ESQUEMA = StructType([
    StructField("despacho_id", StringType()),
    StructField("departamento", StringType()),
    StructField("municipio", StringType()),
    StructField("tipo_carbon", StringType()),
    StructField("toneladas", DoubleType()),
    StructField("poder_calorifico", DoubleType()),
    StructField("humedad", DoubleType()),
    StructField("ceniza", DoubleType()),
    StructField("azufre", DoubleType()),
    StructField("timestamp", LongType()),
])


def parsear(df_kafka):
    """Convierte el 'value' binario de Kafka en columnas tipadas."""
    return (df_kafka
            .select(F.from_json(F.col("value").cast("string"), ESQUEMA).alias("d"))
            .select("d.*")
            .withColumn("hora_evento", F.col("timestamp").cast("timestamp"))
            .withColumn("fuera_spec",
                        (F.col("ceniza") > CENIZA_MAX) | (F.col("azufre") > AZUFRE_MAX)))


def estadisticas_por_ventana(despachos):
    return (despachos
            .withWatermark("hora_evento", "2 minutes")
            .groupBy(F.window("hora_evento", "1 minute"), "departamento")
            .agg(F.count("*").alias("viajes"),
                 F.round(F.sum("toneladas"), 1).alias("toneladas"),
                 F.round(F.avg("poder_calorifico"), 0).alias("pc_kcal_kg"),
                 F.round(F.avg("humedad"), 2).alias("humedad_pct"),
                 F.round(F.avg("ceniza"), 2).alias("ceniza_pct"),
                 F.round(F.avg("azufre"), 2).alias("azufre_pct"),
                 F.sum(F.col("fuera_spec").cast("int")).alias("fuera_spec"))
            .select(F.date_format("window.start", "HH:mm").alias("inicio"),
                    F.date_format("window.end", "HH:mm").alias("fin"),
                    "departamento", "viajes", "toneladas", "pc_kcal_kg",
                    "humedad_pct", "ceniza_pct", "azufre_pct", "fuera_spec"))


def alertas(despachos):
    return (despachos.filter(F.col("fuera_spec"))
            .select(F.date_format("hora_evento", "HH:mm:ss").alias("hora"),
                    "despacho_id", "departamento", "municipio", "toneladas",
                    "ceniza", "azufre"))


def main():
    spark = (SparkSession.builder
             .appName("KafkaSparkStreaming_DespachosCarbon")
             .config("spark.sql.shuffle.partitions", "4")
             .getOrCreate())
    spark.sparkContext.setLogLevel("WARN")

    df_kafka = (spark.readStream
                .format("kafka")
                .option("kafka.bootstrap.servers", "localhost:9092")
                .option("subscribe", "despachos_carbon")
                .option("startingOffsets", "latest")
                .load())

    despachos = parsear(df_kafka)

    consulta_stats = (estadisticas_por_ventana(despachos)
                      .orderBy("inicio", F.desc("toneladas"))
                      .writeStream
                      .outputMode("complete")
                      .format("console")
                      .option("truncate", False)
                      .option("numRows", 50)
                      .trigger(processingTime="10 seconds")
                      .queryName("estadisticas_1min")
                      .start())

    consulta_alertas = (alertas(despachos)
                        .writeStream
                        .outputMode("append")
                        .format("console")
                        .option("truncate", False)
                        .trigger(processingTime="10 seconds")
                        .queryName("alertas_calidad")
                        .start())

    print("Consumidor iniciado. Esperando despachos... (Ctrl+C para detener)")
    spark.streams.awaitAnyTermination()


if __name__ == "__main__":
    main()
