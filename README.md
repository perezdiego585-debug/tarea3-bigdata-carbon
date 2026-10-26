# Producción y regalías de carbón en Colombia con Apache Spark

Tarea 3 – Procesamiento de Datos con Apache Spark · Curso Big Data (202016911) · UNAD
Autor: Diego Pérez

## Problema

Analizar la producción de carbón que paga regalías en Colombia: cómo cambia por año, qué departamentos y municipios producen más, cuánto se paga de regalía por tonelada y cómo le va a Córdoba.

## Conjunto de datos

"ANM Volumen de Explotación de Minerales Asociados a Pagos de Regalías", de la Agencia Nacional de Minería, publicado en el Portal de Datos Abiertos de Colombia: https://www.datos.gov.co/d/r85m-vv6c

Cada fila es un municipio, un mineral y un trimestre, con el volumen explotado (toneladas) y las regalías pagadas (pesos). Tiene unos 78.000 registros.

## Archivos

| Archivo | Para qué sirve |
|---|---|
| `descargar_datos.sh` | Descarga el CSV desde datos.gov.co |
| `analisis_batch_carbon.py` | Análisis en batch con PySpark: carga, limpieza, agrupaciones, RDD, Spark SQL y gráficas |
| `kafka_producer.py` | Generador de datos: simula camiones de carbón y los envía a Kafka (adaptado del Anexo 3) |
| `spark_streaming_consumer.py` | Lee los datos de Kafka con Spark Streaming y calcula estadísticas cada minuto (adaptado del Anexo 3) |

## Cómo ejecutarlo

En la máquina virtual con Spark instalado:

```
pip3 install pandas matplotlib kafka-python
bash descargar_datos.sh
spark-submit analisis_batch_carbon.py
```

El análisis muestra las tablas en la consola y guarda dos gráficas: `grafica_produccion_anual.png` y `grafica_departamentos.png`.

## Tiempo real con Kafka (Anexo 3)

Con ZooKeeper y Kafka iniciados, crear el topic:

```
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic despachos_carbon
```

Terminal 1 (productor):

```
python3 kafka_producer.py
```

Terminal 2 (consumidor):

```
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.8 spark_streaming_consumer.py
```

## Nota

El código se desarrolló con apoyo de una herramienta de inteligencia artificial (Claude) y fue ejecutado, probado y analizado por el autor.
