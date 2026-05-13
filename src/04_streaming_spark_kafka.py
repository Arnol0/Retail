"""
=====================================================
RETAIL - SPARK STRUCTURED STREAMING
=====================================================
Autor: Grupo 11 - Módulo Streaming
Descripción: Consume eventos del topic 'retail-eventos'
             desde Kafka y aplica procesamiento en
             tiempo real con micro-batches de 10 segundos.

Funcionalidades:
  - Resumen 1: Total pedidos y monto por distrito (10 seg)
  - Resumen 2: Conteo de cancelaciones por motivo (30 seg)
  - Alerta 1 : Pedidos con retraso > 30 minutos
  - Alerta 2 : Repartidores con calificación < 2.5

Ejecutar: spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.0 spark_streaming.py
=====================================================
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    from_json, col, sum as spark_sum, count,
    avg, when, to_timestamp, window
)
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, IntegerType
)
import logging

# ─────────────────────────────────────────────
# CONFIGURACIÓN
# ─────────────────────────────────────────────
KAFKA_BROKER  = 'localhost:9092'
TOPIC_NAME    = 'retail-eventos'
CHECKPOINT_DIR = '/tmp/retail/checkpoints'

logging.basicConfig(level=logging.WARN)

# ─────────────────────────────────────────────
# ESQUEMA DEL EVENTO (campos comunes + opcionales)
# ─────────────────────────────────────────────
schema = StructType([
    StructField('id_evento',          StringType(),  True),
    StructField('tipo',               StringType(),  True),
    StructField('id_pedido',          StringType(),  True),
    StructField('id_cliente',         StringType(),  True),
    StructField('id_repartidor',      StringType(),  True),
    StructField('distrito',           StringType(),  True),
    StructField('timestamp',          StringType(),  True),
    StructField('monto_total',        DoubleType(),  True),
    StructField('tiempo_estimado',    IntegerType(), True),
    StructField('tiempo_real',        IntegerType(), True),
    StructField('num_items',          IntegerType(), True),
    StructField('calificacion',       DoubleType(),  True),
    StructField('minutos_retraso',    IntegerType(), True),
    StructField('motivo_retraso',     StringType(),  True),
    StructField('motivo_cancelacion', StringType(),  True),
])

# ─────────────────────────────────────────────
# SPARK SESSION
# ─────────────────────────────────────────────
spark = SparkSession.builder \
    .appName('Rapidex_Streaming') \
    .config('spark.sql.shuffle.partitions', '4') \
    .getOrCreate()

spark.sparkContext.setLogLevel('WARN')

print("=" * 55)
print("  RETAIL — SPARK STRUCTURED STREAMING")
print("=" * 55)
print(f"  Kafka Broker : {KAFKA_BROKER}")
print(f"  Topic        : {TOPIC_NAME}")
print(f"  Micro-batch  : 10 segundos")
print("=" * 55)

# ─────────────────────────────────────────────
# LECTURA DEL STREAM DESDE KAFKA
# ─────────────────────────────────────────────
raw_stream = spark.readStream \
    .format('kafka') \
    .option('kafka.bootstrap.servers', KAFKA_BROKER) \
    .option('subscribe', TOPIC_NAME) \
    .option('startingOffsets', 'latest') \
    .load()

# Deserializar el JSON del campo 'value'
eventos = raw_stream \
    .select(from_json(col('value').cast('string'), schema).alias('data')) \
    .select('data.*') \
    .withColumn('event_time', to_timestamp(col('timestamp')))

# ─────────────────────────────────────────────
# RESUMEN 1: Pedidos realizados por distrito
# Micro-batch: cada 10 segundos
# ─────────────────────────────────────────────
pedidos_por_distrito = eventos \
    .filter(col('tipo') == 'PEDIDO_REALIZADO') \
    .groupBy('distrito') \
    .agg(
        count('*').alias('total_pedidos'),
        spark_sum('monto_total').alias('monto_acumulado'),
        avg('num_items').alias('promedio_items')
    )

query1 = pedidos_por_distrito.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', False) \
    .option('numRows', 15) \
    .trigger(processingTime='10 seconds') \
    .queryName('resumen_pedidos_por_distrito') \
    .start()

print("  ✅ Query 1 activa: Pedidos por distrito (cada 10 seg)")

# ─────────────────────────────────────────────
# RESUMEN 2: Cancelaciones por motivo
# Micro-batch: cada 30 segundos
# ─────────────────────────────────────────────
cancelaciones_por_motivo = eventos \
    .filter(col('tipo') == 'PEDIDO_CANCELADO') \
    .groupBy('motivo_cancelacion') \
    .agg(
        count('*').alias('total_cancelaciones'),
        spark_sum('monto_total').alias('monto_perdido')
    )

query2 = cancelaciones_por_motivo.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', False) \
    .trigger(processingTime='30 seconds') \
    .queryName('resumen_cancelaciones') \
    .start()

print("  ✅ Query 2 activa: Cancelaciones por motivo (cada 30 seg)")

# ─────────────────────────────────────────────
# ALERTA 1: Pedidos retrasados más de 30 minutos
# ─────────────────────────────────────────────
alertas_retraso = eventos \
    .filter(
        (col('tipo') == 'PEDIDO_RETRASADO') &
        (col('minutos_retraso') > 30)
    ) \
    .select(
        col('id_pedido'),
        col('id_repartidor'),
        col('distrito'),
        col('minutos_retraso'),
        col('motivo_retraso'),
        col('timestamp')
    )

query3 = alertas_retraso.writeStream \
    .outputMode('append') \
    .format('console') \
    .option('truncate', False) \
    .trigger(processingTime='10 seconds') \
    .queryName('alertas_retraso_critico') \
    .start()

print("  ✅ Query 3 activa: Alertas retraso > 30 min (cada 10 seg)")

# ─────────────────────────────────────────────
# ALERTA 2: Repartidores con calificación baja
# ─────────────────────────────────────────────
alertas_calificacion = eventos \
    .filter(col('tipo') == 'PEDIDO_ENTREGADO') \
    .groupBy('id_repartidor') \
    .agg(
        avg('calificacion').alias('calificacion_promedio'),
        count('*').alias('total_entregas')
    ) \
    .filter(col('calificacion_promedio') < 2.5)

query4 = alertas_calificacion.writeStream \
    .outputMode('complete') \
    .format('console') \
    .option('truncate', False) \
    .trigger(processingTime='30 seconds') \
    .queryName('alertas_calificacion_baja') \
    .start()

print("  ✅ Query 4 activa: Repartidores calificación < 2.5 (cada 30 seg)")
print("\n  🚀 Streaming en ejecución — esperando eventos de Kafka...")
print("  (Presiona Ctrl+C para detener)\n")

# ─────────────────────────────────────────────
# MANTENER EL STREAMING ACTIVO
# ─────────────────────────────────────────────
try:
    spark.streams.awaitAnyTermination()
except KeyboardInterrupt:
    print("\n  ⛔ Streaming detenido por el usuario.")
    for q in spark.streams.active:
        q.stop()
    spark.stop()
    print("  ✅ Spark Session cerrada correctamente.")
