"""
Proyecto:
Retail Chain - Big Data Analytics

Objetivo:
Generar eventos simulados de ventas retail y enviarlos a Kafka.

Este script funciona como productor Kafka.

Topic utilizado:
- retail-events

Comando:
docker compose exec spark python src/02_generate_streaming_events.py --events 600 --delay 0.1
"""

from pathlib import Path
from datetime import datetime
import argparse
import json
import random
import time

import pandas as pd
from confluent_kafka import Producer


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"

KAFKA_TOPIC = "retail-events"

KAFKA_BOOTSTRAP_SERVERS = "broker:19092"


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def load_reference_data() -> dict:
    """
    Carga datasets históricos retail.
    """

    clientes_path = RAW_DIR / "clientes.csv"

    ventas_path = RAW_DIR / "transacciones_ventas.csv"

    productos_path = RAW_DIR / "inventario_productos.json"

    required_files = [
        clientes_path,
        ventas_path,
        productos_path
    ]

    for file_path in required_files:

        if not file_path.exists():

            raise FileNotFoundError(
                f"No se encontró {file_path}. "
                "Primero ejecuta src/01_generate_retail_data.py"
            )

    clientes_df = pd.read_csv(clientes_path)

    ventas_df = pd.read_csv(ventas_path)

    productos_df = pd.read_json(productos_path)

    # ========================================================
    # MAPAS DE REFERENCIA
    # ========================================================

    clientes_map = (
        clientes_df
        .set_index("id_cliente")
        .to_dict("index")
    )

    productos_map = (
        productos_df
        .set_index("id_producto")
        .to_dict("index")
    )

    return {
        "clientes_df": clientes_df,
        "ventas_df": ventas_df,
        "productos_df": productos_df,
        "clientes_map": clientes_map,
        "productos_map": productos_map
    }


def delivery_report(err, msg) -> None:

    if err is not None:

        print(f"Error enviando mensaje: {err}")


# ============================================================
# CREAR EVENTO
# ============================================================

def create_event(event_number: int, reference_data: dict) -> dict:

    ventas_df = reference_data["ventas_df"]

    clientes_map = reference_data["clientes_map"]

    productos_map = reference_data["productos_map"]

    # ========================================================
    # SELECCIONAR VENTA ALEATORIA
    # ========================================================

    venta = ventas_df.sample(1).iloc[0]

    cliente = clientes_map.get(
        venta["id_cliente"],
        {}
    )

    producto = productos_map.get(
        venta["id_producto"],
        {}
    )

    # ========================================================
    # CALCULAR RIESGO
    # ========================================================

    stock_actual = int(
        producto.get("stock_actual", 0)
    )

    stock_minimo = int(
        producto.get("stock_minimo", 0)
    )

    monto_total = round(
        float(venta["monto_total"]),
        2
    )

    if stock_actual <= stock_minimo:

        risk_score = round(
            random.uniform(0.70, 0.95),
            2
        )

    elif monto_total >= 1500:

        risk_score = round(
            random.uniform(0.40, 0.75),
            2
        )

    else:

        risk_score = round(
            random.uniform(0.05, 0.45),
            2
        )

    is_risk = (
        risk_score >= 0.70
    )

    # ========================================================
    # CREAR EVENTO JSON
    # ========================================================

    event = {

        "event_id": f"EVT-{event_number:06d}",

        "transaction_id": venta["id_transaccion"],

        "customer_id": venta["id_cliente"],

        "product_id": venta["id_producto"],

        "customer_name": cliente.get(
            "nombre",
            "desconocido"
        ),

        "product_name": producto.get(
            "nombre",
            "desconocido"
        ),

        "category": producto.get(
            "categoria",
            "desconocido"
        ),

        "city": cliente.get(
            "ciudad",
            "desconocido"
        ),

        "store": venta.get(
            "tienda",
            "desconocido"
        ),

        "quantity": int(
            venta["cantidad"]
        ),

        "unit_price": round(
            float(venta["precio_unitario"]),
            2
        ),

        "total_amount": monto_total,

        "stock_actual": stock_actual,

        "stock_minimo": stock_minimo,

        "risk_score": risk_score,

        "is_risk": bool(is_risk),

        "event_timestamp": datetime.now().isoformat(
            timespec="seconds"
        )
    }

    return event


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def main() -> None:

    parser = argparse.ArgumentParser(
        description="Generador de eventos streaming Retail"
    )

    parser.add_argument(
        "--events",
        type=int,
        default=600,
        help="Cantidad de eventos a enviar"
    )

    parser.add_argument(
        "--delay",
        type=float,
        default=0.1,
        help="Delay entre eventos"
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Productor Kafka - Retail Analytics")
    print("=" * 80)

    print(f"Topic destino: {KAFKA_TOPIC}")

    print(
        f"Kafka bootstrap servers: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Eventos a enviar: "
        f"{args.events}"
    )

    print(
        f"Delay entre eventos: "
        f"{args.delay} segundos"
    )

    print("=" * 80)

    reference_data = load_reference_data()

    producer_config = {
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS
    }

    producer = Producer(producer_config)

    # ========================================================
    # GENERAR EVENTOS
    # ========================================================

    for event_number in range(1, args.events + 1):

        event = create_event(
            event_number,
            reference_data
        )

        message_value = json.dumps(
            event,
            ensure_ascii=False
        )

        producer.produce(
            topic=KAFKA_TOPIC,

            key=event["transaction_id"],

            value=message_value,

            callback=delivery_report
        )

        producer.poll(0)

        if (
            event_number <= 5
            or event_number % 100 == 0
        ):

            print(
                f"Evento enviado "
                f"{event_number}: "
                f"{message_value}"
            )

        time.sleep(args.delay)

    producer.flush()

    print("=" * 80)
    print("Envío de eventos finalizado correctamente")
    print("=" * 80)


if __name__ == "__main__":
    main()