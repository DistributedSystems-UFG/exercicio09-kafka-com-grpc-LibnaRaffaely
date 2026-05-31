"""
Cliente gRPC – Consulta de Temperaturas
=========================================
Conecta ao servidor gRPC e permite consultar:
  1. Última leitura bruta
  2. Última média de 2 horas
  3. Histórico de leituras
  4. Histórico de médias

Arquitetura:
  server.py  ↔  client.py (gRPC :50051)
"""

import os
import sys
import time

import grpc

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "generated"))
import temperature_pb2
import temperature_pb2_grpc

GRPC_SERVER = "localhost:50051"

SENSORS = ["sensor_01", "sensor_02", "sensor_03"]



def fmt_ts(ts: int) -> str:
    return time.strftime("%Y-%m-%d %H:%M:%S", time.localtime(ts))


def print_reading(r: temperature_pb2.SensorReading) -> None:
    print(f"  Sensor   : {r.sensor_id}")
    print(f"  Temp.    : {r.temperature:.2f} °C")
    print(f"  Horário  : {fmt_ts(r.timestamp)}")


def print_average(a: temperature_pb2.TemperatureAverage) -> None:
    print(f"  Sensor   : {a.sensor_id}")
    print(f"  Média    : {a.average:.2f} °C")
    print(f"  Amostras : {a.sample_count}")
    print(f"  Janela   : {fmt_ts(a.window_start)}  →  {fmt_ts(a.window_end)}")


def choose_sensor() -> str:
    print("  Sensores disponíveis: sensor_01 | sensor_02 | sensor_03 | (vazio = qualquer)")
    return input("  Sensor ID: ").strip()


def menu() -> None:
    channel = grpc.insecure_channel(GRPC_SERVER)
    stub = temperature_pb2_grpc.TemperatureServiceStub(channel)

    while True:
        print("\n╔══════════════════════════════════════╗")
        print("║   Sistema de Monitoramento de Temp.  ║")
        print("╠══════════════════════════════════════╣")
        print("║ 1. Última leitura bruta               ║")
        print("║ 2. Última média (janela 2h)            ║")
        print("║ 3. Histórico de leituras               ║")
        print("║ 4. Histórico de médias                 ║")
        print("║ 0. Sair                                ║")
        print("╚══════════════════════════════════════╝")
        choice = input("Opção: ").strip()

        if choice == "0":
            print("Encerrando cliente.")
            break

        elif choice == "1":
            sensor_id = choose_sensor()
            try:
                r = stub.GetLatestReading(
                    temperature_pb2.QueryRequest(sensor_id=sensor_id)
                )
                print("\n--- Última Leitura ---")
                print_reading(r)
            except grpc.RpcError as e:
                print(f"  [Erro] {e.details()}")

        elif choice == "2":
            sensor_id = choose_sensor()
            try:
                a = stub.GetLatestAverage(
                    temperature_pb2.QueryRequest(sensor_id=sensor_id)
                )
                print("\n--- Última Média (2h) ---")
                print_average(a)
            except grpc.RpcError as e:
                print(f"  [Erro] {e.details()}")

        elif choice == "3":
            sensor_id = choose_sensor()
            try:
                print("\n--- Histórico de Leituras ---")
                count = 0
                for r in stub.GetReadingHistory(
                    temperature_pb2.HistoryRequest(sensor_id=sensor_id)
                ):
                    print(
                        f"  {fmt_ts(r.timestamp)}  {r.sensor_id:10s}  {r.temperature:6.2f} °C"
                    )
                    count += 1
                if count == 0:
                    print("  Nenhum registro encontrado.")
                else:
                    print(f"  Total: {count} leitura(s)")
            except grpc.RpcError as e:
                print(f"  [Erro] {e.details()}")

        elif choice == "4":
            sensor_id = choose_sensor()
            try:
                print("\n--- Histórico de Médias ---")
                count = 0
                for a in stub.GetAverageHistory(
                    temperature_pb2.HistoryRequest(sensor_id=sensor_id)
                ):
                    print(
                        f"  {fmt_ts(a.window_end)}  {a.sensor_id:10s}  "
                        f"média={a.average:6.2f} °C  amostras={a.sample_count}"
                    )
                    count += 1
                if count == 0:
                    print("  Nenhum registro encontrado.")
                else:
                    print(f"  Total: {count} registro(s)")
            except grpc.RpcError as e:
                print(f"  [Erro] {e.details()}")

        else:
            print("  Opção inválida.")


if __name__ == "__main__":
    menu()
