from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, to_date, when, upper, trim, round as spark_round,
    regexp_replace, year, month, dayofweek, count
)

spark = SparkSession.builder \
    .appName("RetailBigData_02_Limpieza") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

spark.sparkContext.setLogLevel("WARN")

print("=" * 60)
print("  PASO 1 — LEER DATOS DESDE HDFS")
print("=" * 60)

df_ventas    = spark.read.csv("hdfs://namenode:9000/retail/raw/ventas/",     header=True, inferSchema=True)
df_clientes  = spark.read.csv("hdfs://namenode:9000/retail/raw/clientes/",   header=True, inferSchema=True)
df_inventario = spark.read.json("hdfs://namenode:9000/retail/raw/inventario/")
df_campanas  = spark.read.json("hdfs://namenode:9000/retail/raw/marketing/")

print(f"  Ventas cargadas       : {df_ventas.count()} registros")
print(f"  Clientes cargados     : {df_clientes.count()} registros")
print(f"  Inventario cargado    : {df_inventario.count()} productos")
print(f"  Campañas cargadas     : {df_campanas.count()} campañas")

# ─────────────────────────────────────────────────────────────
# LIMPIEZA — VENTAS
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("  PASO 2 — LIMPIEZA DE TRANSACCIONES")
print("=" * 60)

total_antes = df_ventas.count()

# Eliminar nulos en campos clave
df_ventas = df_ventas.dropna(subset=["id_cliente", "id_producto", "monto_total"])

# Filtrar montos inválidos
df_ventas = df_ventas.filter(col("monto_total") > 0)

# Normalizar fecha a tipo Date
df_ventas = df_ventas.withColumn("fecha", to_date(col("fecha"), "yyyy-MM-dd"))

# Agregar columnas derivadas de fecha
df_ventas = df_ventas \
    .withColumn("anio",       year(col("fecha"))) \
    .withColumn("mes",        month(col("fecha"))) \
    .withColumn("dia_semana", dayofweek(col("fecha")))

# Normalizar tienda: mayúsculas y sin espacios extra
df_ventas = df_ventas.withColumn("tienda", trim(col("tienda")))

# Calcular monto_unitario real (por si difiere)
df_ventas = df_ventas.withColumn(
    "monto_calculado",
    spark_round(col("precio_unitario") * col("cantidad"), 2)
)

total_despues = df_ventas.count()
descartados   = total_antes - total_despues
pct_descarte  = round((descartados / total_antes) * 100, 2)

print(f"  Registros antes       : {total_antes}")
print(f"  Registros después     : {total_despues}")
print(f"  Descartados           : {descartados} ({pct_descarte}%)")

# ─────────────────────────────────────────────────────────────
# LIMPIEZA — CLIENTES
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("  PASO 3 — ENRIQUECIMIENTO DE CLIENTES")
print("=" * 60)

# Normalizar segmento
df_clientes = df_clientes.withColumn("segmento", upper(trim(col("segmento"))))

# Calcular frecuencia de compra de cada cliente
frecuencia = df_ventas.groupBy("id_cliente") \
    .agg(count("*").alias("num_compras"))

# Unir con clientes
df_clientes = df_clientes.join(frecuencia, on="id_cliente", how="left")
df_clientes = df_clientes.fillna({"num_compras": 0})

# Crear segmento de valor basado en número de compras
df_clientes = df_clientes.withColumn(
    "segmento_valor",
    when(col("num_compras") >= 15, "VIP")
    .when(col("num_compras") >= 8,  "Frecuente")
    .when(col("num_compras") >= 3,  "Regular")
    .otherwise("Ocasional")
)

print("  Distribución de segmento_valor:")
df_clientes.groupBy("segmento_valor").count().orderBy("count", ascending=False).show()

# ─────────────────────────────────────────────────────────────
# LIMPIEZA — INVENTARIO
# ─────────────────────────────────────────────────────────────
print()
print("=" * 60)
print("  PASO 4 — LIMPIEZA DE INVENTARIO")
print("=" * 60)

# Normalizar categoría
df_inventario = df_inventario.withColumn("categoria", trim(col("categoria")))

# Calcular si tiene stock bajo
df_inventario = df_inventario.withColumn(
    "estado_stock",
    when(col("stock_actual") == 0, "AGOTADO")
    .when(col("stock_actual") < col("stock_minimo"), "CRITICO")
    .when(col("stock_actual") < col("stock_minimo") * 2, "BAJO")
    .otherwise("NORMAL")
)

print("  Estado de stock general:")
df_inventario.groupBy("estado_stock").count().orderBy("count", ascending=False).show()

# ─────────────────────────────────────────────────────────────
# GUARDAR DATAFRAMES LIMPIADOS COMO VISTAS TEMPORALES
# ─────────────────────────────────────────────────────────────
df_ventas.createOrReplaceTempView("ventas")
df_clientes.createOrReplaceTempView("clientes")
df_inventario.createOrReplaceTempView("inventario")
df_campanas.createOrReplaceTempView("campanas")

print()
print("=" * 60)
print("  ✅ Vistas temporales registradas:")
print("     ventas | clientes | inventario | campanas")
print("  ✅ Script 02 completado exitosamente")
print("=" * 60)

# Guardar referencias para el siguiente script (sin cerrar spark)
df_ventas.write.mode("overwrite").parquet("hdfs://namenode:9000/retail/clean/ventas/")
df_clientes.write.mode("overwrite").parquet("hdfs://namenode:9000/retail/clean/clientes/")
df_inventario.write.mode("overwrite").parquet("hdfs://namenode:9000/retail/clean/inventario/")
df_campanas.write.mode("overwrite").parquet("hdfs://namenode:9000/retail/clean/campanas/")
print("  💾 Datos limpios guardados en HDFS (/retail/clean/)")

spark.stop()
