from pyspark.sql import SparkSession
from pyspark import SparkContext

spark = SparkSession.builder \
    .appName("RetailBigData_04_RDD_Logs") \
    .config("spark.hadoop.fs.defaultFS", "hdfs://namenode:9000") \
    .getOrCreate()

sc = spark.sparkContext
sc.setLogLevel("WARN")

print("=" * 60)
print("  PASO 1 — LEER LOGS COMO RDD DESDE HDFS")
print("=" * 60)

# Leer archivo de texto como RDD (una línea = un elemento)
rdd_raw = sc.textFile("hdfs://namenode:9000/retail/raw/logs/")
total_lineas = rdd_raw.count()
print(f"  Total líneas de log: {total_lineas}")
print()
print("  Ejemplo de línea:")
print(" ", rdd_raw.first())

# ─────────────────────────────────────────────
# FUNCIÓN AUXILIAR: parsear una línea de log
# Formato: [timestamp] [NIVEL] [TIENDA] [EMPLEADO] mensaje
# ─────────────────────────────────────────────
def parsear_log(linea):
    """Retorna diccionario con campos del log o None si no parsea."""
    try:
        partes = linea.split("] [")
        timestamp = partes[0].replace("[", "").strip()
        nivel     = partes[1].strip()
        tienda    = partes[2].strip()
        resto     = partes[3].split("] ")
        empleado  = resto[0].strip()
        mensaje   = resto[1].strip() if len(resto) > 1 else ""
        return (nivel, tienda, empleado, mensaje, timestamp)
    except Exception:
        return None

# ─────────────────────────────────────────────
# ANÁLISIS 1: Conteo de eventos por NIVEL
# ─────────────────────────────────────────────
print()
print("=" * 60)
print("  ANÁLISIS 1 — EVENTOS POR NIVEL (map + reduceByKey)")
print("=" * 60)

rdd_nivel = rdd_raw \
    .map(parsear_log) \
    .filter(lambda x: x is not None) \
    .map(lambda x: (x[0], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print("  Nivel       | Cantidad")
print("  " + "-" * 30)
for nivel, cantidad in rdd_nivel.collect():
    barra = "█" * (cantidad // 30)
    print(f"  {nivel:<10}  | {cantidad:>5}  {barra}")

# ─────────────────────────────────────────────
# ANÁLISIS 2: Errores por TIENDA
# ─────────────────────────────────────────────
print()
print("=" * 60)
print("  ANÁLISIS 2 — ERRORES POR TIENDA (filter + reduceByKey)")
print("=" * 60)

rdd_errores = rdd_raw \
    .map(parsear_log) \
    .filter(lambda x: x is not None and x[0] == "ERROR") \
    .map(lambda x: (x[1], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

errores_tienda = rdd_errores.collect()
print("  Tienda              | Errores")
print("  " + "-" * 35)
for tienda, errores in errores_tienda:
    barra = "█" * errores
    print(f"  {tienda:<20}| {errores:>4}  {barra}")

# ─────────────────────────────────────────────
# ANÁLISIS 3: Mensajes de ERROR más frecuentes
# ─────────────────────────────────────────────
print()
print("=" * 60)
print("  ANÁLISIS 3 — TIPOS DE ERROR MÁS FRECUENTES")
print("=" * 60)

rdd_tipos_error = rdd_raw \
    .map(parsear_log) \
    .filter(lambda x: x is not None and x[0] == "ERROR") \
    .map(lambda x: (x[3], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False)

print("  Tipo de Error                          | Ocurrencias")
print("  " + "-" * 50)
for msg, cnt in rdd_tipos_error.collect():
    print(f"  {msg:<40}| {cnt:>4}")

# ─────────────────────────────────────────────
# ANÁLISIS 4: Empleados con más WARN o ERROR
# ─────────────────────────────────────────────
print()
print("=" * 60)
print("  ANÁLISIS 4 — EMPLEADOS CON MÁS INCIDENTES (WARN+ERROR)")
print("=" * 60)

rdd_empleados = rdd_raw \
    .map(parsear_log) \
    .filter(lambda x: x is not None and x[0] in ("WARN", "ERROR")) \
    .map(lambda x: (x[2], 1)) \
    .reduceByKey(lambda a, b: a + b) \
    .sortBy(lambda x: x[1], ascending=False) \
    .take(10)

print("  Empleado  | Incidentes")
print("  " + "-" * 25)
for emp, cnt in rdd_empleados:
    print(f"  {emp:<10}| {cnt:>4}")

# ─────────────────────────────────────────────
# Guardar resultado de errores por tienda
# ─────────────────────────────────────────────
print()
print("=" * 60)
print("  💾 GUARDANDO RESULTADO EN HDFS")
print("=" * 60)

# Convertir RDD a DataFrame y guardar
schema = ["tienda", "errores"]
df_errores = spark.createDataFrame(errores_tienda, schema=schema)
df_errores.coalesce(1).write.mode("overwrite").csv(
    "hdfs://namenode:9000/retail/output/errores_tienda/", header=True
)
print("  ✅ errores_tienda guardado en hdfs:///retail/output/errores_tienda/")
print()
print("=" * 60)
print("  ✅ Script 04 — Procesamiento RDD completado exitosamente")
print("=" * 60)

spark.stop()
