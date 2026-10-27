¿De qué trata el proyecto?

En este trabajo quiero analizar la producción de carbón en Colombia y las regalías que se generan por su explotación. Me interesa conocer cómo ha cambiado la producción con los años, cuáles son los departamentos y municipios que más producen y qué participación tiene Córdoba frente a otras regiones del país

Datos utilizados

Para realizar el análisis seleccioné la información publicada por la Agencia Nacional de Minería en el portal Datos Abiertos de Colombia.

Fuente de datos: https://www.datos.gov.co/d/r85m-vv6c

Herramientas

Python

Apache Spark

Apache Kafka

Spark Streaming

Archivos del proyecto

descargar_datos.sh: descarga los datos que voy a utilizar.

analisis_batch_carbon.py: organiza y analiza la información de producción y regalías.

kafka_producer.py: genera datos simulados de camiones que transportan carbón.

spark_streaming_consumer.py: recibe esos datos y calcula estadísticas en tiempo real.

requirements.txt: contiene las librerías de Python utilizadas.

¿Qué espero obtener?

Con este proyecto espero identificar las tendencias de producción de carbón, comparar los resultados por departamento y revisar el comportamiento de Córdoba. También pondré en práctica el procesamiento de datos por lotes y en tiempo real, como parte de la actividad de Big Data.

Autor

Diego Pérez

Big Data
