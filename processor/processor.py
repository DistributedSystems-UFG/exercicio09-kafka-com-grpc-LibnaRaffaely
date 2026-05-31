"""
Processador – Consumidor/Produtor Kafka
========================================
Consome leituras brutas de 'temperature-readings', calcula a média móvel
das últimas 2 horas por sensor e publica o resultado em 'temperature-averages'.

Arquitetura:
  sensor.py  →  [Kafka: temperature-readings]
                        ↓
                   processor.py  →  [Kafka: temperature-averages]  →  server.py
"""

import json
import time
from collections import defaultdict

from kafka import KafkaConsumer, KafkaProducer
from kafka.errors import NoBrokersAvailable

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
INPUT_TOPIC = "temperature-readings"
OUTPUT_TOPIC = "temperature-averages"
CONSUMER_GROUP = "temperature-processor"

# Janela de tempo para o cálculo da média (em segundos)
WINDOW_SECONDS = 2 * 3600  # 2 horas


def create_consumer(retries: int = 10, delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                INPUT_TOPIC,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id=CONSUMER_GROUP,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            print(f"[Processor] Conectado ao Kafka como '{CONSUMER_GROUP}'")
            return consumer
        except NoBrokersAvailable:
            print(f"[Processor] Aguardando Kafka... tentativa {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Não foi possível conectar ao Kafka.")


def create_producer() -> KafkaProducer:
    return KafkaProducer(
        bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
        value_serializer=lambda v: json.dumps(v).encode("utf-8"),
    )


def process() -> None:
    consumer = create_consumer()
    producer = create_producer()

    windows: dict[str, list[tuple[int, float]]] = defaultdict(list)

    print(f"[Processor] Consumindo '{INPUT_TOPIC}' → publicando em '{OUTPUT_TOPIC}'")
    print("-" * 60)

    for message in consumer:
        reading: dict = message.value
        sensor_id: str = reading["sensor_id"]
        temperature: float = reading["temperature"]
        timestamp: int = reading["timestamp"]

        windows[sensor_id].append((timestamp, temperature))

        cutoff = int(time.time()) - WINDOW_SECONDS
        windows[sensor_id] = [
            (ts, temp) for ts, temp in windows[sensor_id] if ts >= cutoff
        ]

        window = windows[sensor_id]
        if not window:
            continue

        avg_temp = sum(temp for _, temp in window) / len(window)
        window_start = min(ts for ts, _ in window)
        window_end = max(ts for ts, _ in window)

        average_event = {
            "sensor_id": sensor_id,
            "average": round(avg_temp, 2),
            "window_start": window_start,
            "window_end": window_end,
            "sample_count": len(window),
        }

        producer.send(OUTPUT_TOPIC, value=average_event)
        producer.flush()

        ts_fmt = time.strftime("%H:%M:%S")
        print(
            f"[{ts_fmt}] {sensor_id}: média={avg_temp:.2f} °C "
            f"({len(window)} amostras na janela de 2h)"
        )


if __name__ == "__main__":
    process()
