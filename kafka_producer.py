# -*- coding: utf-8 -*-
"""
Productor Kafka - Tarea 3 (Big Data UNAD)
Simula, en tiempo real, los despachos de camiones de carbón que salen de
municipios productores de Colombia, con su peso en báscula y su calidad
(poder calorífico, humedad, cenizas y azufre). Envía un evento JSON por
segundo (o cada --intervalo segundos) al topic 'despachos_carbon'.

Uso:  python3 kafka_producer.py              (Ctrl+C para detener)
      python3 kafka_producer.py --intervalo 0.5
"""

import argparse
import json
import random
import time
import uuid

from kafka import KafkaProducer

# Municipios productores de carbón (pesos aproximados para que el simulador
# envíe más camiones desde las zonas que más producen).
MUNICIPIOS = [
    # (departamento, municipio, tipo de carbón, peso)
    ("CESAR", "LA JAGUA DE IBIRICO", "TERMICO", 18),
    ("CESAR", "EL PASO", "TERMICO", 14),
    ("LA GUAJIRA", "BARRANCAS", "TERMICO", 16),
    ("LA GUAJIRA", "ALBANIA", "TERMICO", 8),
    ("CORDOBA", "PUERTO LIBERTADOR", "TERMICO", 7),
    ("CORDOBA", "MONTELIBANO", "TERMICO", 3),
    ("BOYACA", "SOGAMOSO", "METALURGICO", 6),
    ("BOYACA", "SAMACA", "METALURGICO", 5),
    ("CUNDINAMARCA", "LENGUAZAQUE", "METALURGICO", 5),
    ("CUNDINAMARCA", "GUACHETA", "METALURGICO", 4),
    ("NORTE DE SANTANDER", "CUCUTA", "METALURGICO", 5),
    ("ANTIOQUIA", "AMAGA", "TERMICO", 3),
    ("VALLE DEL CAUCA", "JAMUNDI", "TERMICO", 2),
]
PESOS = [m[3] for m in MUNICIPIOS]


def generar_despacho():
    departamento, municipio, tipo, _ = random.choices(MUNICIPIOS, weights=PESOS, k=1)[0]
    if tipo == "TERMICO":
        poder_calorifico = random.gauss(6200, 450)   # kcal/kg
        ceniza = random.gauss(11, 3.5)               # %
        azufre = random.gauss(0.8, 0.3)              # %
    else:
        poder_calorifico = random.gauss(7100, 350)
        ceniza = random.gauss(9, 2.5)
        azufre = random.gauss(0.9, 0.3)
    return {
        "despacho_id": uuid.uuid4().hex[:10],
        "departamento": departamento,
        "municipio": municipio,
        "tipo_carbon": tipo,
        "toneladas": round(random.uniform(28, 36), 2),          # peso neto en báscula
        "poder_calorifico": round(max(poder_calorifico, 3500), 0),
        "humedad": round(max(random.gauss(9, 3), 1), 2),
        "ceniza": round(max(ceniza, 2), 2),
        "azufre": round(max(azufre, 0.2), 2),
        "timestamp": int(time.time()),                           # segundos epoch
    }


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--servidor", default="localhost:9092")
    parser.add_argument("--topic", default="despachos_carbon")
    parser.add_argument("--intervalo", type=float, default=1.0, help="segundos entre eventos")
    args = parser.parse_args()

    producer = KafkaProducer(
        bootstrap_servers=[args.servidor],
        value_serializer=lambda x: json.dumps(x).encode("utf-8"),
        key_serializer=lambda k: k.encode("utf-8"),
    )
    print(f"Enviando despachos al topic '{args.topic}' en {args.servidor} (Ctrl+C para detener)")
    enviados = 0
    try:
        while True:
            despacho = generar_despacho()
            # La clave es el departamento: con varias particiones, Kafka manda
            # todos los camiones de un mismo departamento a la misma partición.
            producer.send(args.topic, key=despacho["departamento"], value=despacho)
            enviados += 1
            print(f"Enviado #{enviados}: {despacho}")
            time.sleep(args.intervalo)
    except KeyboardInterrupt:
        print(f"\nProductor detenido. Despachos enviados: {enviados}")
    finally:
        producer.flush()
        producer.close()


if __name__ == "__main__":
    main()
