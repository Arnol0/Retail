"""
Proyecto:
Retail Chain - Big Data Analytics

Objetivo:
Exportar datasets procesados y KPIs desde Spark hacia MongoDB.

Datasets exportados:
- ventas_enriquecidas
- top_productos
- ventas_por_ciudad
- segmentacion_clientes
- stock_critico
- tiendas_ingresos

Comando:
docker compose exec spark python src/06_export_mongodb.py
"""

from pathlib import Path

from pyspark.sql import SparkSession


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

PROCESSED_DIR = BASE_DIR / "data" / "processed"

KPI_DIR = BASE_DIR / "output" / "kpis"

MONGO_URI = "mongodb://mongodb:27017"

DATABASE_NAME = "retail_db"


# ============================================================
# CREAR SESIÓN SPARK
# ============================================================

def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("RetailMongoExport")
        .master("local[*]")

        .config(
            "spark.jars.packages",
            "org.mongodb.spark:mongo-spark-connector_2.13:10.3.0"
        )

        .config(
            "spark.mongodb.write.connection.uri",
            MONGO_URI
        )

        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# EXPORTAR DATAFRAME A MONGODB
# ============================================================

def export_to_mongodb(df, collection_name: str) -> None:

    (
        df.write
        .format("mongodb")
        .mode("overwrite")
        .option("database", DATABASE_NAME)
        .option("collection", collection_name)
        .save()
    )

    print(f"Colección exportada: {collection_name}")


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def main() -> None:

    print("=" * 80)
    print("Exportando datos Retail a MongoDB")
    print("=" * 80)

    spark = create_spark_session()

    # --------------------------------------------------------
    # DATASET PRINCIPAL
    # --------------------------------------------------------

    ventas_df = spark.read.parquet(
        str(PROCESSED_DIR / "ventas_enriquecidas.parquet")
    )

    export_to_mongodb(
        ventas_df,
        "ventas_enriquecidas"
    )

    # --------------------------------------------------------
    # KPIs CSV
    # --------------------------------------------------------

    kpis = {
        "top_productos_vendidos.csv": "top_productos",
        "ventas_por_ciudad.csv": "ventas_por_ciudad",
        "segmentacion_clientes.csv": "segmentacion_clientes",
        "productos_stock_critico.csv": "stock_critico",
        "tiendas_mayores_ingresos.csv": "tiendas_ingresos"
    }

    for csv_file, collection_name in kpis.items():

        path = KPI_DIR / csv_file

        df = (
            spark.read
            .option("header", True)
            .option("inferSchema", True)
            .csv(str(path))
        )

        export_to_mongodb(df, collection_name)

    print("=" * 80)
    print("Exportación finalizada correctamente")
    print("Base de datos MongoDB: retail_db")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()