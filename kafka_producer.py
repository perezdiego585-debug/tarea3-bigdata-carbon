# Productor Kafka - Tarea 3 Diego perez
# Simula camiones de carbón que salen de los municipios productores
# y envía un dato por segundo al topic "despachos_carbon".
# Ejecutar: python3 kafka_producer.py   

import time
import json
import random
from kafka import KafkaProducer

DEPARTAMENTOS = ["CESAR", "LA GUAJIRA", "CORDOBA", "BOYACA", "CUNDINAMARCA", "NORTE DE SANTANDER"]

def generar_despacho():
    return {
        "camion_id": random.randint(1, 50),
        "departamento": random.choice(DEPARTAMENTOS),
        "toneladas": round(random.uniform(28, 36), 2),
        "ceniza": round(random.uniform(5, 18), 2),
        "humedad": round(random.uniform(5, 15), 2),
        "timestamp": int(time.time())
    }

producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
                         value_serializer=lambda x: json.dumps(x).encode('utf-8'))

while True:
    despacho = generar_despacho()
    producer.send('despachos_carbon', value=despacho)
    print(f"Enviado: {despacho}")
    time.sleep(1)
