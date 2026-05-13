"""
Proyecto:
Retail Chain - Big Data Analytics

Objetivo:
Leer eventos retail desde Kafka usando Spark Structured Streaming,
procesarlos en micro-batches y generar alertas comerciales.

Este script actúa como consumidor streaming.

Entrada:
- Kafka topic: retail-events

Salida:
- Consola de Visual Studio Code
- output/streaming/events/
- output/streaming/alerts/
- output/streaming/summary_by_category.csv
- output/streaming/summary_by_city.csv

Comando:
docker compose exec spark python src/04_streaming_spark_kafka.py --duration 120
"""

from pathlib import Path
import argparse

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType,
    StructField,
    StringType,
    DoubleType,
    IntegerType,
    BooleanType
)


# ============================================================
# CONFIGURACIÓN GENERAL
# ============================================================

BASE_DIR = Path(__file__).resolve().parents[1]

STREAMING_OUTPUT_DIR = BASE_DIR / "output" / "streaming"

EVENTS_OUTPUT_DIR = STREAMING_OUTPUT_DIR / "events"

ALERTS_OUTPUT_DIR = STREAMING_OUTPUT_DIR / "alerts"

CHECKPOINT_DIR = (
    BASE_DIR / "data" / "checkpoints" / "retail_streaming"
)

STREAMING_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
EVENTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
ALERTS_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

KAFKA_TOPIC = "retail-events"

KAFKA_BOOTSTRAP_SERVERS = "broker:19092"

KAFKA_SPARK_PACKAGE = (
    "org.apache.spark:spark-sql-kafka-0-10_2.13:4.1.1"
)


# ============================================================
# ESQUEMA DE EVENTOS
# ============================================================

event_schema = StructType([

    StructField("event_id", StringType(), True),

    StructField("transaction_id", StringType(), True),

    StructField("customer_id", StringType(), True),

    StructField("product_id", StringType(), True),

    StructField("customer_name", StringType(), True),

    StructField("product_name", StringType(), True),

    StructField("category", StringType(), True),

    StructField("city", StringType(), True),

    StructField("store", StringType(), True),

    StructField("quantity", IntegerType(), True),

    StructField("unit_price", DoubleType(), True),

    StructField("total_amount", DoubleType(), True),

    StructField("stock_actual", IntegerType(), True),

    StructField("stock_minimo", IntegerType(), True),

    StructField("risk_score", DoubleType(), True),

    StructField("is_risk", BooleanType(), True),

    StructField("event_timestamp", StringType(), True),
])


# ============================================================
# CREAR SESIÓN SPARK
# ============================================================

def create_spark_session() -> SparkSession:

    spark = (
        SparkSession.builder
        .appName("RetailKafkaStructuredStreaming")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.jars.packages", KAFKA_SPARK_PACKAGE)
        .getOrCreate()
    )

    spark.sparkContext.setLogLevel("WARN")

    return spark


# ============================================================
# FUNCIONES AUXILIARES
# ============================================================

def append_csv_with_header(pdf, output_file: Path) -> None:

    write_header = not output_file.exists()

    pdf.to_csv(
        output_file,
        mode="a",
        header=write_header,
        index=False,
        encoding="utf-8"
    )


# ============================================================
# PROCESAMIENTO DE MICRO-BATCHES
# ============================================================

def process_batch(batch_df, batch_id: int) -> None:

    if batch_df.isEmpty():

        print(f"Batch {batch_id}: sin eventos nuevos")

        return

    print("\n" + "=" * 80)
    print(f"Procesando batch streaming: {batch_id}")
    print("=" * 80)

    batch_df.cache()

    total_events = batch_df.count()

    print(f"Total de eventos recibidos: {total_events}")

    print("\nVista previa de eventos:")

    batch_df.select(
        "transaction_id",
        "customer_name",
        "product_name",
        "category",
        "city",
        "quantity",
        "total_amount",
        "risk_score",
        "is_risk"
    ).show(10, truncate=False)

    # ========================================================
    # RESUMEN POR CATEGORÍA
    # ========================================================

    summary_category_df = (
        batch_df
        .groupBy("category")
        .agg(
            F.count("*").alias("total_ventas"),
            F.round(
                F.sum("total_amount"),
                2
            ).alias("ingresos_totales"),
            F.round(
                F.avg("risk_score"),
                2
            ).alias("riesgo_promedio")
        )
        .orderBy(F.desc("ingresos_totales"))
    )

    print("\nResumen por categoría:")

    summary_category_df.show(truncate=False)

    # ========================================================
    # RESUMEN POR CIUDAD
    # ========================================================

    summary_city_df = (
        batch_df
        .groupBy("city")
        .agg(
            F.count("*").alias("total_ventas"),
            F.round(
                F.sum("total_amount"),
                2
            ).alias("ingresos_totales"),
            F.round(
                F.avg("risk_score"),
                2
            ).alias("riesgo_promedio")
        )
        .orderBy(F.desc("ingresos_totales"))
    )

    print("\nResumen por ciudad:")

    summary_city_df.show(truncate=False)

    # ========================================================
    # ALERTAS
    # ========================================================

    alerts_df = (
        batch_df
        .filter(
            (F.col("is_risk") == True)
            | (F.col("risk_score") >= 0.70)
            | (F.col("stock_actual") <= F.col("stock_minimo"))
        )
        .select(
            "transaction_id",
            "customer_name",
            "product_name",
            "category",
            "city",
            "total_amount",
            "stock_actual",
            "stock_minimo",
            "risk_score",
            "event_timestamp"
        )
        .orderBy(F.desc("risk_score"))
    )

    total_alerts = alerts_df.count()

    print("\nAlertas detectadas:")
    print(f"Total de alertas: {total_alerts}")

    if total_alerts > 0:
        alerts_df.show(10, truncate=False)

    # ========================================================
    # EXPORTAR RESULTADOS
    # ========================================================

    events_pdf = batch_df.toPandas()

    events_pdf["batch_id"] = batch_id

    events_pdf.to_csv(
        EVENTS_OUTPUT_DIR / f"events_batch_{batch_id}.csv",
        index=False,
        encoding="utf-8"
    )

    if total_alerts > 0:

        alerts_pdf = alerts_df.toPandas()

        alerts_pdf["batch_id"] = batch_id

        alerts_pdf.to_csv(
            ALERTS_OUTPUT_DIR / f"alerts_batch_{batch_id}.csv",
            index=False,
            encoding="utf-8"
        )

    summary_category_pdf = summary_category_df.toPandas()

    summary_category_pdf["batch_id"] = batch_id

    append_csv_with_header(
        summary_category_pdf,
        STREAMING_OUTPUT_DIR / "summary_by_category.csv"
    )

    summary_city_pdf = summary_city_df.toPandas()

    summary_city_pdf["batch_id"] = batch_id

    append_csv_with_header(
        summary_city_pdf,
        STREAMING_OUTPUT_DIR / "summary_by_city.csv"
    )

    batch_df.unpersist()

    print(f"\nArchivos generados para batch {batch_id}")

    print(
        f"- output/streaming/events/events_batch_{batch_id}.csv"
    )

    if total_alerts > 0:

        print(
            f"- output/streaming/alerts/alerts_batch_{batch_id}.csv"
        )

    print("- output/streaming/summary_by_category.csv")

    print("- output/streaming/summary_by_city.csv")


# ============================================================
# FUNCIÓN PRINCIPAL
# ============================================================

def main() -> None:

    parser = argparse.ArgumentParser(
        description="Retail Streaming Consumer"
    )

    parser.add_argument(
        "--duration",
        type=int,
        default=120,
        help="Duración del streaming en segundos"
    )

    args = parser.parse_args()

    print("=" * 80)
    print("Spark Structured Streaming - Retail Analytics")
    print("=" * 80)

    print(f"Kafka topic: {KAFKA_TOPIC}")

    print(
        f"Kafka bootstrap servers: "
        f"{KAFKA_BOOTSTRAP_SERVERS}"
    )

    print(
        f"Duración del proceso: "
        f"{args.duration} segundos"
    )

    print("=" * 80)

    spark = create_spark_session()

    # ========================================================
    # LEER STREAM DESDE KAFKA
    # ========================================================

    kafka_stream_df = (
        spark.readStream
        .format("kafka")
        .option(
            "kafka.bootstrap.servers",
            KAFKA_BOOTSTRAP_SERVERS
        )
        .option("subscribe", KAFKA_TOPIC)
        .option("startingOffsets", "latest")
        .load()
    )

    # ========================================================
    # PARSEAR JSON
    # ========================================================

    parsed_events_df = (

        kafka_stream_df

        .select(
            F.col("key").cast("string").alias("message_key"),

            F.col("value").cast("string").alias("message_value"),

            F.col("timestamp").alias("kafka_timestamp")
        )

        .withColumn(
            "json_data",
            F.from_json(
                F.col("message_value"),
                event_schema
            )
        )

        .select(
            "message_key",
            "kafka_timestamp",
            "json_data.*"
        )

        .withColumn(
            "event_timestamp",
            F.to_timestamp("event_timestamp")
        )

        .withColumn(
            "processing_timestamp",
            F.current_timestamp()
        )
    )

    # ========================================================
    # INICIAR STREAMING
    # ========================================================

    query = (

        parsed_events_df

        .writeStream

        .foreachBatch(process_batch)

        .option(
            "checkpointLocation",
            str(CHECKPOINT_DIR)
        )

        .trigger(processingTime="10 seconds")

        .start()
    )

    print("\nStreaming iniciado correctamente")

    query.awaitTermination(args.duration)

    query.stop()

    print("=" * 80)
    print("Streaming finalizado correctamente")
    print("Revisa output/streaming/")
    print("=" * 80)

    spark.stop()


if __name__ == "__main__":
    main()