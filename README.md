# Producción y regalías de carbón en Colombia con Apache Spark y Kafka

Tarea 3 – Procesamiento de Datos con Apache Spark · Curso Big Data (202016911) · UNAD

## Problema

Colombia es uno de los principales productores de carbón de América Latina y las regalías de este mineral financian buena parte del Sistema General de Regalías. Este proyecto responde, con Apache Spark, las siguientes preguntas a partir de los datos oficiales de la Agencia Nacional de Minería:

1. ¿Cómo ha evolucionado año a año la producción de carbón que paga regalías y cuánto dinero ha generado?
2. ¿Qué departamentos y municipios concentran la producción?
3. ¿Cuánto se paga de regalía por cada tonelada y cómo cambia en el tiempo?
4. ¿Hay estacionalidad trimestral en la producción?
5. ¿Qué papel tiene el departamento de Córdoba (Puerto Libertador, Montelíbano) frente a los grandes productores?

Para el componente en tiempo real se simulan los despachos de camiones de carbón desde los municipios productores (peso en báscula y calidad del carbón) y se monitorean con Kafka y Spark Structured Streaming.

## Conjunto de datos

| Campo | Detalle |
|---|---|
| Nombre | ANM Volúmen de Explotación de Minerales Asociados a Pagos de Regalías |
| Fuente | Agencia Nacional de Minería – Portal de Datos Abiertos de Colombia |
| Enlace | https://www.datos.gov.co/d/r85m-vv6c |
| Licencia | Creative Commons Atribución-CompartirIgual 4.0 |
| Actualización | Trimestral |
| Granularidad | Una fila por municipio, mineral, trimestre y año liquidado |

Columnas principales: `codigo_dane`, `nombre_municipio`, `nombre_departamento`, `recurso_natural`, `trimestre`, `a_o_liquidado`, `unidad_medida`, `contraprestacion`, `regalias_pagadas`, `volumenes_de_explotacion`, `min_agrupado2`.

Los valores numéricos llegan como texto con separadores de miles (`"1,721,567.00"`) y los ceros como `"- 0"`, por eso el script incluye una etapa de limpieza.

## Estructura del repositorio

```
tarea3-bigdata-carbon/
├── README.md
├── requirements.txt
├── descargar_datos.sh              # descarga el CSV de datos.gov.co
├── analisis_batch_carbon.py        # procesamiento en batch (PySpark)
└── streaming/
    ├── kafka_producer.py           # generador de despachos de carbón -> Kafka
    └── spark_streaming_consumer.py # Spark Structured Streaming <- Kafka
```

## Qué hace el análisis en batch

| Etapa | Operaciones Spark |
|---|---|
| Carga | `spark.read.csv` con encabezado; normalización de nombres de columna |
| Calidad | conteo de nulos y vacíos por columna; valores negativos; regalías sin volumen |
| Limpieza | quitar tildes y espacios, convertir texto a número, extraer el trimestre, eliminar nulos y duplicados |
| Transformación | filtro de carbón, clasificación térmico/metalúrgico, columna `periodo` |
| EDA | regalías por grupo de mineral; producción y regalías por año con variación % (`Window` + `lag`); ranking de departamentos y municipios; estacionalidad trimestral; regalía por tonelada; detalle de Córdoba |
| RDD | `filter`, `map`, `reduceByKey`, `count`, `takeOrdered`, `reduce` (y comprobación contra el DataFrame) |
| Spark SQL | concentración: % de la producción en los dos mayores departamentos por año |
| Salida | 8 tablas CSV en `resultados/tablas/` y 7 gráficas PNG en `resultados/graficas/` |

## Ejecución

### Requisitos

Máquina virtual Ubuntu con Java, Apache Spark 3.5.x (y opcionalmente Hadoop) configurados en las tareas anteriores. Conéctese por SSH (PuTTY) con el usuario `vboxuser`.

```bash
pip install -r requirements.txt
```

### 1. Obtener el código y los datos

```bash
git clone https://github.com/<su-usuario>/tarea3-bigdata-carbon.git
cd tarea3-bigdata-carbon
bash descargar_datos.sh
```

### 2. (Opcional) Subir los datos a HDFS

```bash
start-dfs.sh
hdfs dfs -mkdir -p /Tarea3
hdfs dfs -put -f regalias_minerales.csv /Tarea3/
```

### 3. Ejecutar el análisis en batch

Desde archivo local:

```bash
spark-submit analisis_batch_carbon.py --input regalias_minerales.csv --output resultados
```

Desde HDFS (ajuste el puerto al de su `core-site.xml`):

```bash
spark-submit analisis_batch_carbon.py --input hdfs://localhost:9000/Tarea3/regalias_minerales.csv --output resultados
```

Para analizar otro departamento en detalle: `--departamento "Boyacá"`.

Mientras se ejecuta, la interfaz web de Spark queda en `http://<ip-de-la-vm>:4040` (Jobs, Stages, Executors).

Para ver las gráficas desde Windows, cópielas con `pscp` o WinSCP desde `~/tarea3-bigdata-carbon/resultados/graficas/`.

## Procesamiento en tiempo real (Anexo 3 adaptado)

Use tres sesiones de PuTTY a la misma VM.

### Instalar y arrancar Kafka (una sola vez)

```bash
pip install kafka-python
wget https://downloads.apache.org/kafka/3.9.2/kafka_2.12-3.9.2.tgz
# Si responde 404, la versión fue archivada:
# wget https://archive.apache.org/dist/kafka/3.9.2/kafka_2.12-3.9.2.tgz
tar -xzf kafka_2.12-3.9.2.tgz
sudo mv kafka_2.12-3.9.2 /opt/Kafka

sudo /opt/Kafka/bin/zookeeper-server-start.sh /opt/Kafka/config/zookeeper.properties &
# Enter cuando termine de imprimir
sudo /opt/Kafka/bin/kafka-server-start.sh /opt/Kafka/config/server.properties &
# Enter cuando termine de imprimir
```

### Crear el topic

```bash
/opt/Kafka/bin/kafka-topics.sh --create --bootstrap-server localhost:9092 \
  --replication-factor 1 --partitions 3 --topic despachos_carbon
/opt/Kafka/bin/kafka-topics.sh --describe --bootstrap-server localhost:9092 --topic despachos_carbon
```

Se usan 3 particiones y la clave del mensaje es el departamento, de modo que todos los camiones de un mismo departamento caen en la misma partición.

### Terminal 1 – Productor

```bash
cd ~/tarea3-bigdata-carbon/streaming
python3 kafka_producer.py
```

### Terminal 2 – Consumidor

Primero verifique la versión de Spark y de Scala:

```bash
spark-submit --version
```

El paquete del conector debe coincidir con esas dos versiones. Para Spark 3.5.3 compilado con Scala 2.12:

```bash
cd ~/tarea3-bigdata-carbon/streaming
spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.3 spark_streaming_consumer.py
```

Si su Spark dice `Scala 2.13`, cambie `_2.12` por `_2.13`; si la versión de Spark es otra, cambie `3.5.3`.

Cada 10 segundos el consumidor imprime dos tablas:

- `estadisticas_1min`: por ventana de 1 minuto y departamento, viajes, toneladas y promedios de poder calorífico, humedad, cenizas y azufre, con el número de viajes fuera de especificación.
- `alertas_calidad`: cada camión con cenizas > 15 % o azufre > 1,2 %.

Detenga ambos scripts con `Ctrl+C`.

### Problemas frecuentes

| Síntoma | Causa probable y solución |
|---|---|
| `NoBrokersAvailable` en el productor | Kafka no arrancó. Revise que ZooKeeper esté arriba antes del broker. |
| `InconsistentClusterIdException` al iniciar Kafka | Restos de una instalación previa: `sudo rm -rf /tmp/kafka-logs` y reinicie el broker. |
| `Failed to find data source: kafka` | Falta `--packages` o la versión del conector no coincide con Spark/Scala. |
| `--packages` se queda descargando | La VM necesita internet la primera vez; luego queda en caché en `~/.ivy2`. |
| La tabla del consumidor sale vacía | Inicie el productor antes que el consumidor (el consumidor lee desde los mensajes más recientes). |

## Resultados esperados

Tras el batch, `resultados/` contiene:

- `tablas/carbon_anual.csv`, `carbon_departamentos.csv`, `carbon_top_municipios.csv`, `carbon_trimestral.csv`, `carbon_tipos.csv`, `carbon_cordoba_anual.csv`, `concentracion_top2.csv`, `regalias_por_grupo_mineral.csv`
- `graficas/01_produccion_regalias_anual.png` … `07_regalias_por_mineral.png`

El año en curso aparece incompleto porque la ANM publica los datos por trimestre liquidado.

## Autor

Diego Pérez – UNAD, curso Big Data 202016911, 2026.
