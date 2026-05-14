"""
Proyecto:
Retail Chain - Big Data Analytics

Objetivo:
Generar gráficos a partir de los reportes KPI creados con Apache Spark.

Este script lee archivos CSV desde output/kpis/
y genera imágenes PNG en output/charts/.

Gráficos generados:
- top_productos_vendidos.png
- ventas_por_ciudad.png
- segmentacion_clientes.png
- productos_stock_critico.png
- tiendas_mayores_ingresos.png

Comando:
docker compose exec spark python src/05_visualizations.py
"""

from pathlib import Path

import pandas as pd
import matplotlib.pyplot as plt


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

KPI_DIR = BASE_DIR / "output" / "kpis"

CHARTS_DIR = BASE_DIR / "output" / "charts"

CHARTS_DIR.mkdir(parents=True, exist_ok=True)


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def read_kpi_csv(filename: str) -> pd.DataFrame:

    path = KPI_DIR / filename

    if not path.exists():
        raise FileNotFoundError(
            f"No se encontró el archivo {path}. "
            "Primero ejecuta src/03_batch_etl_spark.py"
        )

    return pd.read_csv(path)


def save_chart(filename: str) -> None:

    output_path = CHARTS_DIR / filename

    plt.savefig(output_path, bbox_inches="tight", dpi=140)

    plt.close()

    print(f"Gráfico creado: output/charts/{filename}")


# ============================================================
# GRÁFICO 1 - TOP PRODUCTOS VENDIDOS
# ============================================================

def chart_top_productos() -> None:

    df = read_kpi_csv("top_productos_vendidos.csv")

    top_df = df.sort_values(
        "ingresos_totales",
        ascending=False
    )

    plt.figure(figsize=(10, 6))

    plt.bar(
        top_df["categoria"],
        top_df["ingresos_totales"]
    )

    plt.title("Ingresos Totales Por Categoría")
    plt.xlabel("Categoría")
    plt.ylabel("Ingresos Totales")

    for index, value in enumerate(top_df["ingresos_totales"]):
        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("top_productos_vendidos.png")


# ============================================================
# GRÁFICO 2 - VENTAS POR CIUDAD
# ============================================================

def chart_ventas_por_ciudad() -> None:

    df = read_kpi_csv("ventas_por_ciudad.csv")

    top_df = df.sort_values(
        "ingresos_totales",
        ascending=False
    ).head(10)

    plt.figure(figsize=(11, 6))

    plt.bar(
        top_df["ciudad"],
        top_df["ingresos_totales"]
    )

    plt.title("Top 10 Ciudades Con Mayores Ventas")
    plt.xlabel("Ciudad")
    plt.ylabel("Ingresos Totales")

    plt.xticks(rotation=45, ha="right")

    for index, value in enumerate(top_df["ingresos_totales"]):
        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("ventas_por_ciudad.png")


# ============================================================
# GRÁFICO 3 - SEGMENTACIÓN CLIENTES
# ============================================================

def chart_segmentacion_clientes() -> None:

    df = read_kpi_csv("segmentacion_clientes.csv")

    plt.figure(figsize=(9, 5))

    plt.bar(
        df["segmento"],
        df["gasto_total"]
    )

    plt.title("Gasto Total Por Segmento De Cliente")
    plt.xlabel("Segmento")
    plt.ylabel("Gasto Total")

    for index, value in enumerate(df["gasto_total"]):
        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("segmentacion_clientes.png")


# ============================================================
# GRÁFICO 4 - STOCK CRÍTICO
# ============================================================

def chart_stock_critico() -> None:

    df = read_kpi_csv("productos_stock_critico.csv")

    top_df = df.head(10)

    plt.figure(figsize=(12, 6))

    plt.bar(
        top_df["nombre_producto"],
        top_df["stock_actual"]
    )

    plt.title("Productos Con Stock Crítico")
    plt.xlabel("Producto")
    plt.ylabel("Stock Actual")

    plt.xticks(rotation=45, ha="right")

    for index, value in enumerate(top_df["stock_actual"]):
        plt.text(
            index,
            value,
            str(value),
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("productos_stock_critico.png")


# ============================================================
# GRÁFICO 5 - TIENDAS CON MÁS INGRESOS
# ============================================================

def chart_tiendas_ingresos() -> None:

    df = read_kpi_csv("tiendas_mayores_ingresos.csv")

    top_df = df.sort_values(
        "ingresos_totales",
        ascending=False
    )

    plt.figure(figsize=(10, 6))

    plt.bar(
        top_df["tienda"],
        top_df["ingresos_totales"]
    )

    plt.title("Tiendas Con Mayores Ingresos")
    plt.xlabel("Tienda")
    plt.ylabel("Ingresos Totales")

    for index, value in enumerate(top_df["ingresos_totales"]):
        plt.text(
            index,
            value,
            f"{value:,.0f}",
            ha="center",
            va="bottom",
            fontsize=8
        )

    save_chart("tiendas_mayores_ingresos.png")


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def main() -> None:

    print("=" * 80)
    print("Generando visualizaciones Retail Analytics")
    print("=" * 80)

    chart_top_productos()

    chart_ventas_por_ciudad()

    chart_segmentacion_clientes()

    chart_stock_critico()

    chart_tiendas_ingresos()

    print("=" * 80)
    print("Visualizaciones generadas correctamente")
    print("Carpeta de salida: output/charts/")
    print("=" * 80)


if __name__ == "__main__":
    main()