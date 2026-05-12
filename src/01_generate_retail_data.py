"""
01_generate_retail_data.py

Proyecto:
Retail Chain - Big Data Analytics

Objetivo:
Generar datos sintéticos para análisis retail:
- clientes
- ventas
- inventario
- campañas
- logs

comando: docker compose exec spark python src/01_generate_retail_data.py
Salida:
data/raw/
"""

from pathlib import Path
from datetime import datetime, timedelta
import random
import json

import numpy as np
import pandas as pd
from faker import Faker


# ============================================================
# CONFIGURACIÓN
# ============================================================

SEED = 2026

random.seed(SEED)
np.random.seed(SEED)

fake = Faker("es_ES")
Faker.seed(SEED)

# Cantidades
N_CLIENTES = 20_000
N_PRODUCTOS = 5_000
N_TRANSACCIONES = 100_000
N_CAMPANAS = 2_000
N_LOGS = 50_000

BASE_DIR = Path(__file__).resolve().parents[1]

RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def random_datetime(start_date, end_date):
    total_seconds = int((end_date - start_date).total_seconds())
    random_seconds = random.randint(0, total_seconds)
    return start_date + timedelta(seconds=random_seconds)


# ============================================================
# CLIENTES
# ============================================================

def generate_clientes():

    segmentos = ["Premium", "Regular", "Nuevo"]
    ciudades = [
        "Lima",
        "Arequipa",
        "Cusco",
        "Trujillo",
        "Piura",
        "Chiclayo"
    ]

    rows = []

    for i in range(1, N_CLIENTES + 1):

        rows.append({
            "id_cliente": f"CLI-{i:06d}",
            "nombre": fake.first_name(),
            "apellido": fake.last_name(),
            "edad": random.randint(18, 70),
            "ciudad": random.choice(ciudades),
            "segmento": random.choices(
                segmentos,
                weights=[0.20, 0.60, 0.20],
                k=1
            )[0],
            "email": f"cliente{i}@retail.pe"
        })

    return pd.DataFrame(rows)


# ============================================================
# PRODUCTOS
# ============================================================

def generate_productos():

    categorias = {
        "Calzado": ["Zapatillas", "Botas", "Sandalias"],
        "Ropa": ["Polo", "Casaca", "Jean"],
        "Accesorios": ["Mochila", "Reloj", "Correa"],
        "Deportes": ["Pelota", "Guantes", "Bicicleta"],
        "Hogar": ["Silla", "Mesa", "Lámpara"]
    }

    proveedores = [
        "Nike Perú",
        "Adidas Perú",
        "Puma Retail",
        "Importaciones SAC",
        "Distribuidora Andina"
    ]

    rows = []

    for i in range(1, N_PRODUCTOS + 1):

        categoria = random.choice(list(categorias.keys()))
        nombre = random.choice(categorias[categoria])

        precio = round(random.uniform(20, 800), 2)

        stock_actual = random.randint(0, 500)

        rows.append({
            "id_producto": f"PROD-{i:06d}",
            "nombre": nombre,
            "categoria": categoria,
            "precio": precio,
            "stock_actual": stock_actual,
            "stock_minimo": random.randint(10, 40),
            "proveedor": random.choice(proveedores)
        })

    return rows


# ============================================================
# TRANSACCIONES
# ============================================================

def generate_transacciones(clientes_df, productos):

    tiendas = [
        "Mall Aventura",
        "Jockey Plaza",
        "Real Plaza",
        "Mega Plaza",
        "Open Plaza"
    ]

    start_date = datetime(2024, 1, 1)
    end_date = datetime(2025, 12, 31)

    clientes_ids = clientes_df["id_cliente"].tolist()

    rows = []

    for i in range(1, N_TRANSACCIONES + 1):

        producto = random.choice(productos)

        cantidad = random.randint(1, 5)

        precio = producto["precio"]

        monto_total = round(precio * cantidad, 2)

        rows.append({
            "id_transaccion": f"TRX-{i:08d}",
            "id_cliente": random.choice(clientes_ids),
            "id_producto": producto["id_producto"],
            "cantidad": cantidad,
            "precio_unitario": precio,
            "monto_total": monto_total,
            "fecha": random_datetime(start_date, end_date),
            "tienda": random.choice(tiendas)
        })

    return pd.DataFrame(rows)


# ============================================================
# CAMPAÑAS
# ============================================================

def generate_campanas():

    segmentos = ["Premium", "Regular", "Nuevo"]
    canales = ["email", "SMS", "push"]

    rows = []

    for i in range(1, N_CAMPANAS + 1):

        fecha_inicio = fake.date_between(
            start_date="-2y",
            end_date="today"
        )

        fecha_fin = fecha_inicio + timedelta(
            days=random.randint(5, 30)
        )

        rows.append({
            "id_campana": f"CMP-{i:05d}",
            "nombre": f"Campaña {i}",
            "segmento_objetivo": random.choice(segmentos),
            "canal": random.choice(canales),
            "fecha_inicio": str(fecha_inicio),
            "fecha_fin": str(fecha_fin),
            "descuento_porcentaje": random.choice(
                [5, 10, 15, 20, 25, 30]
            )
        })

    return rows


# ============================================================
# LOGS
# ============================================================

def generate_logs():

    niveles = ["INFO", "WARNING", "ERROR"]

    tiendas = [
        "Mall Aventura",
        "Jockey Plaza",
        "Real Plaza"
    ]

    eventos = [
        "APERTURA_CAJA",
        "DEVOLUCION_PRODUCTO",
        "ALERTA_STOCK",
        "CIERRE_TURNO",
        "ERROR_SISTEMA"
    ]

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 12, 31)

    logs = []

    for _ in range(N_LOGS):

        timestamp = random_datetime(start_date, end_date)

        line = (
            f"[{timestamp}] "
            f"[{random.choice(niveles)}] "
            f"[{random.choice(tiendas)}] "
            f"[{random.choice(eventos)}] "
            f"[Detalle automático generado]"
        )

        logs.append(line)

    return logs


# ============================================================
# SAVE FILES
# ============================================================

def save_csv(df, filename):

    path = RAW_DIR / filename

    df.to_csv(path, index=False)

    print(f"{filename} generado")


def save_json(data, filename):

    path = RAW_DIR / filename

    with open(path, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)

    print(f"{filename} generado")


def save_txt(lines, filename):

    path = RAW_DIR / filename

    with open(path, "w", encoding="utf-8") as f:

        for line in lines:
            f.write(line + "\n")

    print(f"{filename} generado")


# ============================================================
# MAIN
# ============================================================

def main():

    print("Generando datasets retail...")

    clientes_df = generate_clientes()
    save_csv(clientes_df, "clientes.csv")

    productos = generate_productos()
    save_json(productos, "inventario_productos.json")

    transacciones_df = generate_transacciones(
        clientes_df,
        productos
    )

    save_csv(
        transacciones_df,
        "transacciones_ventas.csv"
    )

    campanas = generate_campanas()
    save_json(
        campanas,
        "campanas_marketing.json"
    )

    logs = generate_logs()
    save_txt(
        logs,
        "logs_actividad_tienda.txt"
    )

    print("Proceso finalizado.")


if __name__ == "__main__":
    main()