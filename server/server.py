"""
Servidor gRPC – Consumidor Kafka + Web Service
================================================
Consome eventos dos tópicos Kafka e armazena em SQLite.
Expõe um serviço gRPC para que clientes consultem leituras e médias.

Arquitetura:
  [Kafka: temperature-readings]  ──┐
                                    ├──  server.py  ↔  client.py
  [Kafka: temperature-averages]  ──┘    (gRPC :50051)
"""

import json
import os
import sqlite3
import sys
import threading
import time
from concurrent import futures

import grpc
from kafka import KafkaConsumer
from kafka.errors import NoBrokersAvailable

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generated"))
import temperature_pb2
import temperature_pb2_grpc

KAFKA_BOOTSTRAP_SERVERS = "localhost:9092"
READINGS_TOPIC = "temperature-readings"
AVERAGES_TOPIC = "temperature-averages"
GRPC_PORT = 50051
DB_PATH = os.path.join(os.path.dirname(__file__), "temperature.db")


def init_db() -> None:
    conn = sqlite3.connect(DB_PATH)
    c = conn.cursor()
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS readings (
            id         INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id  TEXT    NOT NULL,
            temperature REAL   NOT NULL,
            timestamp  INTEGER NOT NULL
        )
        """
    )
    c.execute(
        """
        CREATE TABLE IF NOT EXISTS averages (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            sensor_id    TEXT    NOT NULL,
            average      REAL    NOT NULL,
            window_start INTEGER NOT NULL,
            window_end   INTEGER NOT NULL,
            sample_count INTEGER NOT NULL
        )
        """
    )
    conn.commit()
    conn.close()


def insert_reading(reading: dict) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        "INSERT INTO readings (sensor_id, temperature, timestamp) VALUES (?, ?, ?)",
        (reading["sensor_id"], reading["temperature"], reading["timestamp"]),
    )
    conn.commit()
    conn.close()


def insert_average(avg: dict) -> None:
    conn = sqlite3.connect(DB_PATH)
    conn.execute(
        """
        INSERT INTO averages (sensor_id, average, window_start, window_end, sample_count)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            avg["sensor_id"],
            avg["average"],
            avg["window_start"],
            avg["window_end"],
            avg["sample_count"],
        ),
    )
    conn.commit()
    conn.close()



def _make_consumer(topic: str, group: str, retries: int = 15, delay: int = 3) -> KafkaConsumer:
    for attempt in range(1, retries + 1):
        try:
            consumer = KafkaConsumer(
                topic,
                bootstrap_servers=KAFKA_BOOTSTRAP_SERVERS,
                value_deserializer=lambda v: json.loads(v.decode("utf-8")),
                group_id=group,
                auto_offset_reset="earliest",
                enable_auto_commit=True,
            )
            return consumer
        except NoBrokersAvailable:
            print(f"[Server] Aguardando Kafka ({topic})... tentativa {attempt}/{retries}")
            time.sleep(delay)
    raise RuntimeError("Não foi possível conectar ao Kafka.")


def consume_readings() -> None:
    consumer = _make_consumer(READINGS_TOPIC, "server-readings-group")
    print(f"[Server] Consumindo leituras de '{READINGS_TOPIC}'")
    for message in consumer:
        reading = message.value
        insert_reading(reading)
        ts = time.strftime("%H:%M:%S")
        print(f"[{ts}] [DB] Leitura: {reading['sensor_id']} = {reading['temperature']:.2f} °C")


def consume_averages() -> None:
    consumer = _make_consumer(AVERAGES_TOPIC, "server-averages-group")
    print(f"[Server] Consumindo médias de '{AVERAGES_TOPIC}'")
    for message in consumer:
        avg = message.value
        insert_average(avg)
        ts = time.strftime("%H:%M:%S")
        print(
            f"[{ts}] [DB] Média: {avg['sensor_id']} = {avg['average']:.2f} °C "
            f"({avg['sample_count']} amostras)"
        )


class TemperatureServicer(temperature_pb2_grpc.TemperatureServiceServicer):

    def GetLatestReading(self, request, context):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        if request.sensor_id:
            c.execute(
                "SELECT sensor_id, temperature, timestamp FROM readings "
                "WHERE sensor_id = ? ORDER BY timestamp DESC LIMIT 1",
                (request.sensor_id,),
            )
        else:
            c.execute(
                "SELECT sensor_id, temperature, timestamp FROM readings "
                "ORDER BY timestamp DESC LIMIT 1"
            )
        row = c.fetchone()
        conn.close()

        if row:
            return temperature_pb2.SensorReading(
                sensor_id=row[0], temperature=row[1], timestamp=row[2]
            )
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details("Nenhuma leitura encontrada.")
        return temperature_pb2.SensorReading()

    def GetLatestAverage(self, request, context):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        if request.sensor_id:
            c.execute(
                "SELECT sensor_id, average, window_start, window_end, sample_count "
                "FROM averages WHERE sensor_id = ? ORDER BY window_end DESC LIMIT 1",
                (request.sensor_id,),
            )
        else:
            c.execute(
                "SELECT sensor_id, average, window_start, window_end, sample_count "
                "FROM averages ORDER BY window_end DESC LIMIT 1"
            )
        row = c.fetchone()
        conn.close()

        if row:
            return temperature_pb2.TemperatureAverage(
                sensor_id=row[0],
                average=row[1],
                window_start=row[2],
                window_end=row[3],
                sample_count=row[4],
            )
        context.set_code(grpc.StatusCode.NOT_FOUND)
        context.set_details("Nenhuma média encontrada.")
        return temperature_pb2.TemperatureAverage()

    def GetReadingHistory(self, request, context):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        query = "SELECT sensor_id, temperature, timestamp FROM readings WHERE 1=1"
        params: list = []
        if request.sensor_id:
            query += " AND sensor_id = ?"
            params.append(request.sensor_id)
        if request.from_timestamp:
            query += " AND timestamp >= ?"
            params.append(request.from_timestamp)
        if request.to_timestamp:
            query += " AND timestamp <= ?"
            params.append(request.to_timestamp)
        query += " ORDER BY timestamp ASC"
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()

        for row in rows:
            yield temperature_pb2.SensorReading(
                sensor_id=row[0], temperature=row[1], timestamp=row[2]
            )

    def GetAverageHistory(self, request, context):
        conn = sqlite3.connect(DB_PATH)
        c = conn.cursor()
        query = (
            "SELECT sensor_id, average, window_start, window_end, sample_count "
            "FROM averages WHERE 1=1"
        )
        params: list = []
        if request.sensor_id:
            query += " AND sensor_id = ?"
            params.append(request.sensor_id)
        if request.from_timestamp:
            query += " AND window_end >= ?"
            params.append(request.from_timestamp)
        if request.to_timestamp:
            query += " AND window_end <= ?"
            params.append(request.to_timestamp)
        query += " ORDER BY window_end ASC"
        c.execute(query, params)
        rows = c.fetchall()
        conn.close()

        for row in rows:
            yield temperature_pb2.TemperatureAverage(
                sensor_id=row[0],
                average=row[1],
                window_start=row[2],
                window_end=row[3],
                sample_count=row[4],
            )


def serve() -> None:
    init_db()
    print(f"[Server] Banco de dados inicializado em '{DB_PATH}'")

    # Inicia consumidores Kafka em threads daemon
    threading.Thread(target=consume_readings, daemon=True).start()
    threading.Thread(target=consume_averages, daemon=True).start()

    # Inicia servidor gRPC
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    temperature_pb2_grpc.add_TemperatureServiceServicer_to_server(
        TemperatureServicer(), server
    )
    server.add_insecure_port(f"[::]:{GRPC_PORT}")
    server.start()
    print(f"[Server] Servidor gRPC iniciado na porta {GRPC_PORT}")
    print("=" * 60)
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
