"""
Sensor simulado – Produtor Kafka
=================================
Simula três sensores de temperatura que publicam leituras no tópico
'temperature-readings' sempre que ocorre uma variação significativa (>= 0,5 °C).

Arquitetura:
  (1) sensor.py  →  [Kafka: temperature-readings]  →  processor.py
"""

import json
import random
import time

from kafka import KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
TOPIC = "temperature-readings"

VARIATION_THRESHOLD = 0.5

SENSORS = {
    "sensor_01": 22.0,
    "sensor_02": 18.5,
    "sensor_03": 25.0,
}


def create_producer(retries: int = 10, delay: int = 3) -> KafkaProducer:
    for attempt in range(1, retries + 1):
        try:
            producer = KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
            )
            print(f"[Sensor] Conectado ao Kafka em {KAFKA_BOOTSTRAP_SERVERS}")
            return producer
        except NoBrokersAvailable:
            print(f"[Sensor] Aguardando Kafka... tentativa {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Não foi possível conectar ao Kafka.")


def simulate() -> None:
    producer = create_producer()
    temperatures = dict(SENSORS)

    print(f"[Sensor] Iniciado. Publicando em '{TOPIC}'")
    print("-" * 50)

    while True:
        for sensor_id in list(temperatures):
            variation = random.uniform(-2.0, 2.0)

            # Só publica se a variação for significativa
            if abs(variation) >= VARIATION_THRESHOLD:
                new_temp = temperatures[sensor_id] + variation
                new_temp = round(max(10.0, min(45.0, new_temp)), 2)  # clamp 10–45 °C
                temperatures[sensor_id] = new_temp

                reading = {
                    "sensor_id": sensor_id,
                    "temperature": new_temp,
                    "timestamp": int(time.time()),
                }

                producer.send(TOPIC, value=reading)
                ts = time.strftime("%H:%M:%S")
                print(f"[{ts}] {sensor_id}: {new_temp:6.2f} °C  (Δ{variation:+.2f})")

        producer.flush()
        time.sleep(random.uniform(2, 5))


if __name__ == "__main__":
    simulate()
