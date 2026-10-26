# Tarea 3 - Big Data UNAD
# Análisis en batch con PySpark: producción y regalías de carbón en Colombia
# Datos: Agencia Nacional de Minería (datos.gov.co, r85m-vv6c)
# Ejecutar: spark-submit analisis_batch_carbon.py

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, upper, translate, regexp_replace, sum, round
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

# 1. Iniciar Spark
spark = SparkSession.builder.appName("Tarea3_Regalias_Carbon").getOrCreate()
spark.sparkContext.setLogLevel("WARN")

# 2. Cargar el archivo CSV
df = spark.read.option("header", True).csv("regalias_minerales.csv")
print("Total de registros:", df.count())
df.printSchema()
df.show(5)

# 3. Limpieza
#    - Los números vienen como texto ("1,721,567.00" o "- 0"): se dejan solo dígitos y punto
#    - Los nombres se pasan a mayúsculas y sin tildes
df = df.select(
    upper(translate(col("nombre_departamento"), "ÁÉÍÓÚáéíóú", "AEIOUaeiou")).alias("departamento"),
    upper(translate(col("nombre_municipio"), "ÁÉÍÓÚáéíóú", "AEIOUaeiou")).alias("municipio"),
    upper(col("recurso_natural")).alias("mineral"),
    col("a_o_liquidado").cast("int").alias("anio"),
    regexp_replace(col("volumenes_de_explotacion"), "[^0-9.-]", "").cast("double").alias("toneladas"),
    regexp_replace(col("regalias_pagadas"), "[^0-9.-]", "").cast("double").alias("regalias"),
)
df = df.fillna(0).dropDuplicates()

# 4. Filtrar solo el carbón
carbon = df.filter(col("mineral").contains("CARB"))
print("Registros de carbón:", carbon.count())

# 5. Producción y regalías por año
por_anio = (carbon.groupBy("anio")
            .agg(round(sum("toneladas") / 1000000, 2).alias("millones_toneladas"),
                 round(sum("regalias") / 1000000000, 1).alias("regalias_miles_millones"))
            .withColumn("regalia_por_tonelada",
                        round(col("regalias_miles_millones") * 1000 / col("millones_toneladas"), 0))
            .orderBy("anio"))
print("\nProducción y regalías de carbón por año")
por_anio.show(20)

# 6. Producción por departamento
por_depto = (carbon.groupBy("departamento")
             .agg(round(sum("toneladas") / 1000000, 2).alias("millones_toneladas"))
             .orderBy(col("millones_toneladas").desc()))
print("\nProducción de carbón por departamento")
por_depto.show()

# 7. Detalle de Córdoba por año
print("\nProducción de carbón en Córdoba por año")
(carbon.filter(col("departamento") == "CORDOBA")
       .groupBy("anio")
       .agg(round(sum("toneladas") / 1000, 1).alias("miles_toneladas"))
       .orderBy("anio")
       .show(20))

# 8. Lo mismo con RDD: transformaciones (map, filter, reduceByKey) y acciones (count, take)
rdd = carbon.rdd
toneladas_depto = (rdd.filter(lambda fila: fila["toneladas"] > 0)
                      .map(lambda fila: (fila["departamento"], fila["toneladas"]))
                      .reduceByKey(lambda a, b: a + b))
print("\nRDD - departamentos con producción:", toneladas_depto.count())
print("RDD - primeros 3 resultados:", toneladas_depto.take(3))

# 9. Consulta con Spark SQL: los 10 municipios que más producen
carbon.createOrReplaceTempView("carbon")
print("\nSpark SQL - Top 10 municipios productores de carbón")
spark.sql("""
    SELECT departamento, municipio, ROUND(SUM(toneladas) / 1000000, 2) AS millones_toneladas
    FROM carbon
    GROUP BY departamento, municipio
    ORDER BY millones_toneladas DESC
    LIMIT 10
""").show()

# 10. Gráficas (se guardan como imágenes PNG)
anios = por_anio.toPandas()
plt.figure(figsize=(10, 5))
plt.bar(anios["anio"].astype(str), anios["millones_toneladas"], color="steelblue")
plt.title("Producción de carbón con regalías por año")
plt.ylabel("Millones de toneladas")
plt.xticks(rotation=45)
plt.tight_layout()
plt.savefig("grafica_produccion_anual.png")

deptos = por_depto.limit(8).toPandas()
plt.figure(figsize=(10, 5))
plt.barh(deptos["departamento"], deptos["millones_toneladas"], color="darkorange")
plt.gca().invert_yaxis()
plt.title("Departamentos que más carbón producen (2012-2026)")
plt.xlabel("Millones de toneladas")
plt.tight_layout()
plt.savefig("grafica_departamentos.png")

print("\nGráficas guardadas: grafica_produccion_anual.png y grafica_departamentos.png")
spark.stop()
